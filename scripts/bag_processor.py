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

def save_image_to_memory(msg) -> Optional[PILImage.Image]:
    """
    将 ROS 图像消息保存到内存中
    
    参数:
        msg: ROS 图像消息
        
    返回:
        PIL Image 对象，如果失败则返回 None
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
            return None
            
        # 安全检查数据大小
        data_size = 0
        if hasattr(msg_data, 'size'):
            data_size = msg_data.size
        else:
            data_size = len(msg_data)
            
        if data_size == 0:
            print("  图像数据为空")
            return None
            
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
        
        return image
    except Exception as e:
        print(f"  处理图像时出错: {e}")
        return None

def find_closest_timestamp(target_ts, timestamps):
    """查找最接近的时间戳"""
    if not timestamps:
        return None
    return min(timestamps, key=lambda x: abs(x - target_ts))

def process_synchronized_batch(lidar_data_batch, image_data_batch, lidar_dir, camera_dir, start_index):
    """
    处理一批同步的数据并保存到文件
    """
    # 创建图像子目录
    image_subdir = os.path.join(camera_dir, 'image')
    Path(image_subdir).mkdir(parents=True, exist_ok=True)
    
    # 同步这批数据
    synchronized_batch = []
    lidar_timestamps = sorted(lidar_data_batch.keys())
    image_topics = list(image_data_batch.keys())
    image_timestamps_map = {}
    
    for topic in image_topics:
        image_timestamps_map[topic] = sorted(image_data_batch[topic].keys())
    
    for i, lidar_ts in enumerate(lidar_timestamps):
        synced_images = {}
        
        # 为每个图像话题找到最接近的图像
        for topic in image_topics:
            if not image_timestamps_map[topic]:
                continue
                
            # 找到最接近的图像时间戳
            closest_ts = find_closest_timestamp(lidar_ts, image_timestamps_map[topic])
            if closest_ts:
                synced_images[topic] = image_data_batch[topic][closest_ts]
        
        # 添加同步数据
        synchronized_batch.append((lidar_ts, synced_images, start_index + i))
    
    # 保存同步后的数据到文件
    lidar_saved = 0
    image_saved = 0
    
    for lidar_ts, image_files, index in synchronized_batch:
        # 生成统一的基础文件名（不包含扩展名）
        base_filename = f"{index:06d}"
        
        # 写入同步后的点云文件
        if lidar_ts in lidar_data_batch:
            points = lidar_data_batch[lidar_ts]
            new_lidar_name = f"{base_filename}.pcd"
            new_lidar_path = os.path.join(lidar_dir, new_lidar_name)
            
            write_pcd_file(new_lidar_path, points)
            lidar_saved += 1
        
        # 写入同步后的图像文件
        for topic, img_info in image_files.items():
            if 'data' in img_info:
                new_img_name = f"{base_filename}.jpg"
                new_img_path = os.path.join(image_subdir, new_img_name)
                
                # 保存图像到磁盘
                try:
                    img_info['data'].save(new_img_path)
                    image_saved += 1
                except Exception as e:
                    print(f"  保存图像失败: {e}")
    
    return lidar_saved, image_saved

def process_bag_files(input_dir: str, output_base_dir: str, batch_size: int = 100) -> None:
    """
    处理目录中的所有bag文件，并将提取的数据保存到指定的目录结构中。
    
    参数:
        input_dir: 包含bag文件的输入目录
        output_base_dir: 输出数据的基目录
        batch_size: 批处理大小，控制内存使用量
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
        
        # 初始化传感器数据存储（在内存中处理）
        lidar_data = {}  # {timestamp: point_cloud_data}
        image_data = defaultdict(dict)  # {topic: {timestamp: image_data}}
        
        # 创建图像子目录
        image_subdir = os.path.join(camera_dir, 'image')
        Path(image_subdir).mkdir(parents=True, exist_ok=True)
        
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
            batch_index = 0  # 用于文件命名的全局索引
            
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
                            # 将点云数据存储在内存中而不是立即写入文件
                            lidar_data[timestamp] = points
                    
                    # 处理图像数据
                    elif connection.msgtype in IMAGE_TYPES:
                        # 将图像数据存储在内存中而不是立即写入文件
                        image = save_image_to_memory(msg)
                        if image:
                            image_data[connection.topic][timestamp] = {
                                'data': image,
                                'width': getattr(msg, 'width', 0),
                                'height': getattr(msg, 'height', 0),
                                'encoding': getattr(msg, 'encoding', 'unknown')
                            }
                
                except Exception as e:
                    # 不在进度条中显示错误，避免刷屏
                    continue
                
                processed_messages += 1
                
                # 检查是否达到批次大小，如果是，则处理当前批次并清空内存
                if len(lidar_data) >= batch_size or sum(len(imgs) for imgs in image_data.values()) >= batch_size:
                    if lidar_data and any(image_data.values()):
                        print(f"  处理批次 (内存中有 {len(lidar_data)} 个点云, {sum(len(imgs) for imgs in image_data.values())} 个图像)")
                        
                        # 处理当前批次的同步数据
                        lidar_saved, image_saved = process_synchronized_batch(
                            lidar_data.copy(), 
                            {k: v.copy() for k, v in image_data.items()}, 
                            lidar_dir, 
                            camera_dir, 
                            batch_index
                        )
                        
                        print(f"  批次处理完成: {lidar_saved} 个点云文件和 {image_saved} 个图像文件")
                        
                        # 更新全局索引
                        batch_index += max(len(lidar_data), max([len(imgs) for imgs in image_data.values()] or [0]))
                        
                        # 清空内存
                        lidar_data.clear()
                        for topic in image_data:
                            image_data[topic].clear()
                        
                        # 显式调用垃圾回收
                        import gc
                        gc.collect()
            
            # 处理剩余的数据
            if lidar_data and any(image_data.values()):
                print(f"  处理最后一批数据 (内存中有 {len(lidar_data)} 个点云, {sum(len(imgs) for imgs in image_data.values())} 个图像)")
                
                lidar_saved, image_saved = process_synchronized_batch(
                    lidar_data, 
                    image_data, 
                    lidar_dir, 
                    camera_dir, 
                    batch_index
                )
                
                print(f"  最后一批处理完成: {lidar_saved} 个点云文件和 {image_saved} 个图像文件")
            
            # 如果没有同步数据，但仍需要保存原始数据
            elif lidar_data or any(image_data.values()):
                print("  没有同步数据，保存原始数据...")
                
                # 按原始顺序保存点云数据
                for idx, (ts, points) in enumerate(lidar_data.items()):
                    filename = f"{batch_index + idx:06d}.pcd"
                    filepath = os.path.join(lidar_dir, filename)
                    write_pcd_file(filepath, points)
                
                # 按原始顺序保存图像数据
                img_idx = 0
                for topic_dict in image_data.values():
                    for ts, img_info in topic_dict.items():
                        if 'data' in img_info:
                            new_img_name = f"{batch_index + img_idx:06d}.jpg"
                            new_img_path = os.path.join(image_subdir, new_img_name)
                            try:
                                img_info['data'].save(new_img_path)
                                img_idx += 1
                            except Exception as e:
                                print(f"  保存图像失败: {e}")
            
            # 创建场景描述文件
            desc_file = os.path.join(scene_dir, 'desc.json')
            scene_info = {
                'scene_name': scene_name,
                'bag_file': bag_file.name,
                'processed_date': datetime.now().isoformat(),
                'lidar_count': processed_messages,  # 这里应该是实际处理的总数
                'camera_topics': list(image_data.keys()),
                'synchronized_frames': batch_index  # 实际保存的同步帧数
            }
            
            with open(desc_file, 'w', encoding='utf-8') as f:
                json.dump(scene_info, f, ensure_ascii=False, indent=2)
            
            print(f"  场景描述已保存: {desc_file}")
            print(f"  点云数量: {len(lidar_data)}")
            print(f"  图像话题数量: {len(image_data)}")
            print(f"  同步帧数: {batch_index}")
            
            reader.close()
            
        except Exception as e:
            print(f"处理文件 {bag_file.name} 时出错: {e}")
            if 'reader' in locals():
                reader.close()
            continue
    
    print(f"\n所有bag文件处理完成，结果保存在: {output_base_dir}")

def main():
    """主函数"""
    # 处理data/bags目录中的所有bag文件，设置较小的批次大小以减少内存使用
    process_bag_files('./data/bags', './data', batch_size=50)

if __name__ == "__main__":
    main()