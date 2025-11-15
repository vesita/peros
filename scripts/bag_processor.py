#!/usr/bin/env python3
"""
ROS Bag 处理器

用于将 ROS bag 文件中的点云和图像数据转换为标注工具所需的格式。
点云数据转换为 PCD 格式，图像数据导出为 JPG 文件。
"""

import os
import json
import struct
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from collections import defaultdict
import numpy as np
from tqdm import tqdm

try:
    from rosbags.rosbag1 import Reader as Bag1Reader
    from rosbags.rosbag2 import Reader as Bag2Reader
    from rosbags.typesys import get_typestore, Stores
    # 尝试导入 PIL 用于图像处理
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError as e:
    print("错误：缺少必要的库。")
    print(f"导入错误：{e}")
    print("请使用以下命令安装：pip install rosbags Pillow")
    exit(1)

# 支持的传感器话题类型
LIDAR_TYPES = [
    'sensor_msgs/msg/PointCloud2',
    'sensor_msgs/PointCloud2'
]

IMAGE_TYPES = [
    'sensor_msgs/msg/Image',
    'sensor_msgs/Image'
]

def to_timestamp(ns: int) -> float:
    """
    将纳秒时间戳转换为秒
    
    参数:
        ns: 纳秒时间戳
        
    返回:
        转换后的秒时间戳
    """
    return ns / 1e9

def write_pcd_file(filepath: str, points: List[float], width: int = 1) -> None:
    """
    将点云数据写入 PCD 文件
    
    参数:
        filepath: 输出文件路径
        points: 点云数据 [x1, y1, z1, x2, y2, z2, ...]
        width: 点云宽度
    """
    height = len(points) // 3
    
    with open(filepath, 'w') as f:
        # PCD 文件头
        f.write("# .PCD v0.7 - Point Cloud Data file format\n")
        f.write("VERSION 0.7\n")
        f.write("FIELDS x y z\n")
        f.write("SIZE 4 4 4\n")
        f.write("TYPE F F F\n")
        f.write("COUNT 1 1 1\n")
        f.write(f"WIDTH {height}\n")
        f.write(f"HEIGHT {width}\n")
        f.write("VIEWPOINT 0 0 0 1 0 0 0\n")
        f.write(f"POINTS {height}\n")
        f.write("DATA ascii\n")
        
        # 写入点云数据
        for i in range(0, len(points), 3):
            f.write(f"{points[i]:.6f} {points[i+1]:.6f} {points[i+2]:.6f}\n")

def extract_pointcloud2_data(msg) -> List[float]:
    """
    从 PointCloud2 消息中提取点云数据
    
    参数:
        msg: PointCloud2 消息对象
        
    返回:
        点云数据列表 [x1, y1, z1, x2, y2, z2, ...]
    """
    points = []
    
    # 获取字段信息
    fields = {}
    for field in msg.fields:
        fields[field.name] = field.offset
    
    # 检查是否有点云数据
    msg_data = getattr(msg, 'data', None)
    # 安全检查：使用 size > 0 而不是直接 if 语句以避免 NumPy 数组歧义错误
    if msg_data is None or (hasattr(msg_data, 'size') and msg_data.size == 0) or (not hasattr(msg_data, 'size') and len(msg_data) == 0):
        return points
        
    # 解析点云数据
    point_step = msg.point_step
    row_step = msg.row_step
    data = bytes(msg_data)
    
    # 计算点的数量
    num_points = len(data) // point_step
    
    # 预编译 struct 格式以提高性能
    float_struct = struct.Struct('f')
    
    for i in range(num_points):
        offset = i * point_step
        
        # 提取 x, y, z 坐标
        x_offset = offset + fields.get('x', 0)
        y_offset = offset + fields.get('y', 4)
        z_offset = offset + fields.get('z', 8)
        
        # 解析浮点数
        if x_offset + 4 <= len(data):
            x = float_struct.unpack(data[x_offset:x_offset+4])[0]
            y = float_struct.unpack(data[y_offset:y_offset+4])[0]
            z = float_struct.unpack(data[z_offset:z_offset+4])[0]
            points.extend([x, y, z])
    
    return points

def save_image_data(msg, filepath: str) -> bool:
    """
    将 ROS 图像消息保存为 JPEG 文件
    
    参数:
        msg: ROS 图像消息
        filepath: 输出文件路径
        
    返回:
        bool: 是否保存成功
    """
    try:
        # 获取图像属性
        width = getattr(msg, 'width', 0)
        height = getattr(msg, 'height', 0)
        encoding = getattr(msg, 'encoding', 'unknown')
        msg_data = getattr(msg, 'data', None)
        
        # 检查必要参数
        if width <= 0 or height <= 0 or msg_data is None:
            print(f"  图像参数无效: width={width}, height={height}")
            return False
            
        # 安全检查数据大小
        data_size = 0
        if hasattr(msg_data, 'size'):
            data_size = msg_data.size
        else:
            data_size = len(msg_data)
            
        if data_size == 0:
            print("  图像数据为空")
            return False
            
        # 转换为字节
        data = bytes(msg_data)
        
        # 根据编码格式处理图像
        if encoding in ['rgb8', 'bgr8']:
            # RGB 或 BGR 格式图像
            # 创建 PIL 图像对象
            if encoding == 'rgb8':
                image = PILImage.frombuffer('RGB', (width, height), data, 'raw', 'RGB', 0, 1)
            else:  # bgr8
                image = PILImage.frombuffer('RGB', (width, height), data, 'raw', 'BGR', 0, 1)
        elif encoding in ['rgba8', 'bgra8']:
            # RGBA 或 BGRA 格式图像
            if encoding == 'rgba8':
                image = PILImage.frombuffer('RGBA', (width, height), data, 'raw', 'RGBA', 0, 1)
            else:  # bgra8
                image = PILImage.frombuffer('RGBA', (width, height), data, 'raw', 'BGRA', 0, 1)
        elif encoding in ['mono8', 'mono16']:
            # 单通道灰度图像
            mode = 'L' if encoding == 'mono8' else 'I;16'
            image = PILImage.frombuffer(mode, (width, height), data, 'raw', mode, 0, 1)
        elif encoding.startswith('bayer_'):
            # Bayer 格式图像，需要去马赛克处理
            # 简化处理：先转换为灰度图
            image = PILImage.frombuffer('L', (width, height), data, 'raw', 'L', 0, 1)
        else:
            # 其他编码格式，尝试按灰度图处理
            print(f"  未知的图像编码格式: {encoding}，尝试按灰度图处理")
            image = PILImage.frombuffer('L', (width, height), data, 'raw', 'L', 0, 1)
        
        # 保存为 JPEG 格式
        image.save(filepath, 'JPEG')
        return True
    except Exception as e:
        print(f"  保存图像时出错: {e}")
        # 尝试保存原始数据作为后备方案
        try:
            msg_data = getattr(msg, 'data', None)
            if msg_data is not None:
                with open(filepath, 'wb') as f:
                    f.write(bytes(msg_data))
            return True
        except Exception as e2:
            print(f"  保存原始数据也失败了: {e2}")
            return False

def synchronize_frames(lidar_data, image_data):
    """
    同步激光雷达点云和图像数据帧
    
    参数:
        lidar_data: 激光雷达数据字典 {timestamp: filepath}
        image_data: 图像数据字典 {topic: {timestamp: info}}
        
    返回:
        同步的数据列表 [(lidar_filepath, {topic: image_filepath})]
    """
    if not lidar_data or not image_data:
        return []
    
    # 获取所有激光雷达时间戳并排序
    lidar_timestamps = sorted(lidar_data.keys())
    
    # 获取所有图像话题
    image_topics = list(image_data.keys())
    
    # 为每个图像话题创建时间戳到文件路径的映射
    image_timestamps_map = {}
    for topic in image_topics:
        image_timestamps_map[topic] = sorted(image_data[topic].keys())
    
    # 同步数据
    synchronized_data = []
    
    for i, lidar_ts in enumerate(lidar_timestamps):
        lidar_filepath = lidar_data[lidar_ts]
        synced_images = {}
        
        # 为每个图像话题找到最接近的图像
        for topic in image_topics:
            if not image_timestamps_map[topic]:
                continue
                
            # 找到最接近的图像时间戳
            closest_ts = min(image_timestamps_map[topic], key=lambda x: abs(x - lidar_ts))
            synced_images[topic] = image_data[topic][closest_ts]['path']
        
        # 添加同步数据
        synchronized_data.append((lidar_filepath, synced_images, i))
    
    return synchronized_data

def rename_synchronized_files(synchronized_data, lidar_dir, camera_dir):
    """
    使用统一索引命名重命名点云文件，图像文件已按序号命名无需重命名
    
    参数:
        synchronized_data: 同步数据列表 [(lidar_filepath, {topic: image_filepath}, index)]
        lidar_dir: 点云数据目录
        camera_dir: 图像数据目录
    """
    # 创建图像子目录
    image_subdir = os.path.join(camera_dir, 'image')
    Path(image_subdir).mkdir(parents=True, exist_ok=True)
    
    lidar_renamed = 0
    
    for lidar_filepath, image_files, index in synchronized_data:
        # 生成统一的基础文件名（不包含扩展名）
        base_filename = f"{index:06d}"
        
        # 重命名点云文件
        new_lidar_name = f"{base_filename}.pcd"
        new_lidar_path = os.path.join(lidar_dir, new_lidar_name)
        if os.path.exists(lidar_filepath):
            try:
                os.rename(lidar_filepath, new_lidar_path)
                lidar_renamed += 1
            except Exception as e:
                print(f"  重命名点云文件失败: {e}")
        else:
            print(f"  点云文件不存在，跳过重命名: {lidar_filepath}")
    
    print(f"  成功重命名 {lidar_renamed} 个点云文件")


def process_bag_files(input_dir: str, output_base_dir: str) -> None:
    """
    处理目录中的所有bag文件，并将提取的数据保存到指定的目录结构中。
    
    参数:
        input_dir: 包含bag文件的输入目录
        output_base_dir: 输出数据的基目录
    """
    # 创建基础输出目录
    Path(output_base_dir).mkdir(parents=True, exist_ok=True)
    
    # 获取所有bag文件
    bag_files = []
    for file in Path(input_dir).iterdir():
        if file.is_file() and (file.suffix == '.bag'):
            bag_files.append(file)
    
    if not bag_files:
        print(f"在目录 '{input_dir}' 中未找到任何bag文件")
        return
    
    print(f"找到 {len(bag_files)} 个bag文件")
    
    # 处理每个bag文件
    for bag_file in bag_files:
        print(f"\n正在处理: {bag_file.name}")
        bag_path = str(bag_file)
        
        # 为每个bag文件创建场景目录，以时间戳命名
        scene_name = datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "_" + bag_file.stem
        scene_dir = os.path.join(output_base_dir, scene_name)
        lidar_dir = os.path.join(scene_dir, 'lidar')
        camera_dir = os.path.join(scene_dir, 'camera')
        label_dir = os.path.join(scene_dir, 'label')
        
        # 创建必要的目录
        Path(lidar_dir).mkdir(parents=True, exist_ok=True)
        Path(camera_dir).mkdir(parents=True, exist_ok=True)
        Path(label_dir).mkdir(parents=True, exist_ok=True)
        
        # 初始化传感器数据存储
        lidar_data = {}  # {timestamp: filepath}
        image_data = defaultdict(dict)  # {topic: {timestamp: info}}
        
        # 创建图像子目录
        image_subdir = os.path.join(camera_dir, 'image')
        Path(image_subdir).mkdir(parents=True, exist_ok=True)
        
        # 文件索引计数器
        lidar_index = 0
        image_index = 0
        
        try:
            # 尝试以ROS1 bag格式打开
            is_ros1 = True
            try:
                reader = Bag1Reader(bag_path)
                reader.open()
                typestore = get_typestore(Stores.ROS1_NOETIC)
            except:
                # 尝试以ROS2 bag格式打开
                is_ros1 = False
                reader = Bag2Reader(bag_path)
                reader.open()
                typestore = get_typestore(Stores.ROS2_FOXY)
            
            # 获取消息数量用于进度条
            total_messages = reader.message_count if hasattr(reader, 'message_count') else 0
            
            # 处理所有消息
            progress_desc = f"处理 {bag_file.name}"
            progress_bar = tqdm(
                reader.messages(), 
                total=total_messages if total_messages > 0 else None,
                desc=progress_desc,
                unit="msgs",
                ncols=80,
                leave=True
            )
            
            processed_messages = 0
            for connection, timestamp, rawdata in progress_bar:
                try:
                    # 更新进度条描述，显示最近处理的话题
                    progress_bar.set_description(f"处理 {bag_file.name} [{connection.topic}]", refresh=False)
                    
                    # 反序列化消息
                    if is_ros1:
                        msg = typestore.deserialize_ros1(rawdata, connection.msgtype)
                    else:
                        msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                    
                    # 处理激光雷达数据
                    if connection.msgtype in LIDAR_TYPES:
                        points = extract_pointcloud2_data(msg)
                        if points is not None and len(points) > 0:
                            # 直接使用序号命名
                            filename = f"{lidar_index:06d}.pcd"
                            lidar_index += 1
                            filepath = os.path.join(lidar_dir, filename)
                            
                            write_pcd_file(filepath, points)
                            lidar_data[timestamp] = filepath
                    
                    # 处理图像数据
                    elif connection.msgtype in IMAGE_TYPES:
                        msg_data = getattr(msg, 'data', None)
                        if msg_data is not None and ((hasattr(msg_data, 'size') and msg_data.size > 0) or (not hasattr(msg_data, 'size') and len(msg_data) > 0)):
                            # 直接使用序号命名
                            filename = f"{image_index:06d}.jpg"
                            image_index += 1
                            filepath = os.path.join(image_subdir, filename)
                            
                            if save_image_data(msg, filepath):
                                image_data[connection.topic][timestamp] = {
                                    'path': filepath,
                                    'width': getattr(msg, 'width', 0),
                                    'height': getattr(msg, 'height', 0),
                                    'encoding': getattr(msg, 'encoding', 'unknown')
                                }
                
                except Exception as e:
                    # 不在进度条中显示错误，避免刷屏
                    continue
                
                processed_messages += 1
            
            # 关闭进度条
            progress_bar.close()
            reader.close()
            
            # 执行帧同步和重命名
            print("  正在执行帧同步...")
            synchronized_data = synchronize_frames(lidar_data, image_data)
            
            if synchronized_data:
                print(f"  同步了 {len(synchronized_data)} 帧")
                # 重命名点云文件以匹配同步顺序，图像文件已按序号命名无需重命名
                rename_synchronized_files(synchronized_data, lidar_dir, camera_dir)
            else:
                print("  没有数据需要同步")
            
            # 创建场景描述文件
            desc_file = os.path.join(scene_dir, 'desc.json')
            scene_info = {
                'scene_name': scene_name,
                'bag_file': bag_file.name,
                'processed_date': datetime.now().isoformat(),
                'lidar_count': len(lidar_data),
                'camera_topics': list(image_data.keys()),
                'synchronized_frames': len(synchronized_data)
            }
            
            with open(desc_file, 'w', encoding='utf-8') as f:
                json.dump(scene_info, f, ensure_ascii=False, indent=2)
            
            print(f"  场景描述已保存: {desc_file}")
            print(f"  点云数量: {len(lidar_data)}")
            print(f"  图像话题数量: {len(image_data)}")
            print(f"  同步帧数: {len(synchronized_data)}")
            
        except Exception as e:
            print(f"处理文件 {bag_file.name} 时出错: {e}")
            continue
    
    print(f"\n所有bag文件处理完成，结果保存在: {output_base_dir}")

def main():
    """主函数"""
    # 处理data/bags目录中的所有bag文件
    process_bag_files('./data/bags', './data')

if __name__ == "__main__":
    main()