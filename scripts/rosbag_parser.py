#!/usr/bin/env python3
"""
ROS Bag 解析器

用于将 ROS bag 文件中的数据转换为 SUSTechPOINTS 标注工具所需的文件结构。
该工具专门处理点云和图像数据，将其转换为标准格式。
"""

import argparse
import os
import json
import struct
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from collections import defaultdict

try:
    from rosbags.rosbag1 import Reader as Bag1Reader
    from rosbags.rosbag2 import Reader as Bag2Reader
    from rosbags.typesys import get_typestore, Stores
    # 移除了无法解析的导入，这些类型将在实际使用时通过typestore获取
except ImportError:
    print("错误：未找到 rosbags 库。")
    print("请使用以下命令安装：pip install rosbags")
    exit(1)


def list_topics(bag_path: str) -> None:
    """
    列出 bag 文件中的所有话题。
    
    参数:
        bag_path: bag 文件路径
    """
    try:
        # 首先尝试 ROS1 bag 格式
        with Bag1Reader(bag_path) as reader:
            print(f"ROS1 bag 文件: {bag_path}")
            print("可用话题:")
            for connection in reader.connections:
                print(f"  {connection.topic} ({connection.msgtype})")
            return
    except Exception:
        # 尝试 ROS2 bag 格式
        try:
            with Bag2Reader(bag_path) as reader:
                print(f"ROS2 bag 文件: {bag_path}")
                print("可用话题:")
                for connection in reader.connections:
                    print(f"  {connection.topic} ({connection.msgtype})")
                return
        except Exception as e:
            print(f"读取 bag 文件时出错: {e}")
            return


def safe_str(obj):
    """
    安全地将对象转换为字符串
    
    参数:
        obj: 要转换的对象
        
    返回:
        转换后的字符串
    """
    try:
        return str(obj)
    except:
        return "<无法序列化的对象>"


def extract_generic_data(typestore, reader, connections, output_dir: str, topic_name: str, is_ros1: bool = True) -> None:
    """
    提取通用话题数据
    
    参数:
        typestore: 类型存储
        reader: Bag 读取器
        connections: 连接列表
        output_dir: 输出目录
        topic_name: 话题名称
        is_ros1: 是否为 ROS1 格式
    """
    # 将消息保存到文本文件
    output_file = os.path.join(output_dir, f"{topic_name.replace('/', '_')[1:]}.txt")
    count = 0
    
    with open(output_file, 'w') as f:
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            try:
                # 根据格式选择反序列化方法
                if is_ros1:
                    msg = typestore.deserialize_ros1(rawdata, connection.msgtype)
                else:
                    msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                
                f.write(f"时间戳: {timestamp}\n")
                f.write(f"消息类型: {connection.msgtype}\n")
                
                # 对常见消息类型进行特殊处理
                if 'sensor_msgs/msg/Image' in connection.msgtype or 'sensor_msgs/Image' in connection.msgtype:
                    f.write(f"图像宽度: {getattr(msg, 'width', 'N/A')}\n")
                    f.write(f"图像高度: {getattr(msg, 'height', 'N/A')}\n")
                    f.write(f"编码格式: {getattr(msg, 'encoding', 'N/A')}\n")
                    f.write(f"数据大小: {len(getattr(msg, 'data', []))} 字节\n")
                elif 'sensor_msgs/msg/Imu' in connection.msgtype or 'sensor_msgs/Imu' in connection.msgtype:
                    orientation = getattr(msg, 'orientation', None)
                    if orientation:
                        f.write(f"方向: x={orientation.x}, y={orientation.y}, z={orientation.z}, w={orientation.w}\n")
                    angular_velocity = getattr(msg, 'angular_velocity', None)
                    if angular_velocity:
                        f.write(f"角速度: x={angular_velocity.x}, y={angular_velocity.y}, z={angular_velocity.z}\n")
                    linear_acceleration = getattr(msg, 'linear_acceleration', None)
                    if linear_acceleration:
                        f.write(f"线性加速度: x={linear_acceleration.x}, y={linear_acceleration.y}, z={linear_acceleration.z}\n")
                elif 'sensor_msgs/msg/LaserScan' in connection.msgtype or 'sensor_msgs/LaserScan' in connection.msgtype:
                    f.write(f"扫描角度最小值: {getattr(msg, 'angle_min', 'N/A')}\n")
                    f.write(f"扫描角度最大值: {getattr(msg, 'angle_max', 'N/A')}\n")
                    f.write(f"角度增量: {getattr(msg, 'angle_increment', 'N/A')}\n")
                    ranges = getattr(msg, 'ranges', [])
                    f.write(f"扫描距离范围: {len(ranges)} 个点\n")
                    if ranges:
                        f.write(f"最小距离: {min(ranges):.3f}\n")
                        f.write(f"最大距离: {max(ranges):.3f}\n")
                else:
                    # 默认处理方式，只输出一些关键字段
                    fields = []
                    for attr in dir(msg):
                        if not attr.startswith('_') and not callable(getattr(msg, attr)):
                            fields.append(f"{attr}={safe_str(getattr(msg, attr))}")
                    f.write(f"消息字段: {', '.join(fields[:10])}\n")  # 只显示前10个字段
                    
            except Exception as e:
                f.write(f"时间戳: {timestamp}\n")
                f.write(f"消息类型: {connection.msgtype}\n")
                f.write(f"解析消息时出错: {e}\n")
                f.write(f"原始数据大小: {len(rawdata)} 字节\n")
            
            f.write("-" * 50 + "\n")
            count += 1
    
    print(f"提取了 {count} 条消息，数据保存到 {output_file}")


def extract_image_data(typestore, reader, connections, output_dir: str, topic_name: str, is_ros1: bool = True) -> None:
    """
    提取图像话题数据
    
    参数:
        typestore: 类型存储
        reader: Bag 读取器
        connections: 连接列表
        output_dir: 输出目录
        topic_name: 话题名称
        is_ros1: 是否为 ROS1 格式
    """
    # 创建图像输出目录（符合 SUSTechPOINTS 要求的目录结构）
    image_dir = os.path.join(output_dir, 'camera', 'image')
    Path(image_dir).mkdir(parents=True, exist_ok=True)
    
    # 创建元数据文件
    metadata_file = os.path.join(output_dir, f"{topic_name.replace('/', '_')[1:]}.txt")
    
    count = 0
    with open(metadata_file, 'w') as meta_f:
        meta_f.write(f"图像话题: {topic_name}\n")
        meta_f.write("=" * 50 + "\n")
        
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            try:
                # 根据格式选择反序列化方法
                if is_ros1:
                    msg = typestore.deserialize_ros1(rawdata, connection.msgtype)
                else:
                    msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                
                # 写入元数据
                meta_f.write(f"时间戳: {timestamp}\n")
                meta_f.write(f"图像宽度: {getattr(msg, 'width', 'N/A')}\n")
                meta_f.write(f"图像高度: {getattr(msg, 'height', 'N/A')}\n")
                meta_f.write(f"编码格式: {getattr(msg, 'encoding', 'N/A')}\n")
                meta_f.write(f"步长: {getattr(msg, 'step', 'N/A')}\n")
                meta_f.write(f"数据大小: {len(getattr(msg, 'data', []))} 字节\n")
                
                # 保存图像数据到符合 SUSTechPOINTS 要求的目录结构
                image_filename = f"image_{count:06d}.bin"
                image_path = os.path.join(image_dir, image_filename)
                with open(image_path, 'wb') as img_f:
                    img_f.write(bytes(msg.data))
                
                meta_f.write(f"图像文件: {image_filename}\n")
                meta_f.write("-" * 50 + "\n")
                count += 1
                
            except Exception as e:
                meta_f.write(f"时间戳: {timestamp}\n")
                meta_f.write(f"解析图像消息时出错: {e}\n")
                meta_f.write(f"原始数据大小: {len(rawdata)} 字节\n")
                meta_f.write("-" * 50 + "\n")
                count += 1
    
    print(f"提取了 {count} 张图像，元数据保存到 {metadata_file}，图像文件保存到 {image_dir}")


def extract_topic_data(bag_path: str, topic_name: str, output_dir: str) -> None:
    """
    从 bag 文件中提取指定话题的数据。
    
    参数:
        bag_path: bag 文件路径
        topic_name: 要提取的话题名称
        output_dir: 保存提取数据的目录
    """
    # 如果目录不存在则创建
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    reader = None
    try:
        # 首先尝试 ROS1 bag 格式
        reader = Bag1Reader(bag_path)
        reader.open()
        # 获取类型存储以反序列化消息
        typestore = get_typestore(Stores.ROS1_NOETIC)
        
        # 查找指定话题的连接
        connections = [x for x in reader.connections if x.topic == topic_name]
        if not connections:
            print(f"在 bag 文件中未找到话题 '{topic_name}'")
            reader.close()
            return
        
        # 打印话题类型用于调试
        msgtype = connections[0].msgtype
        print(f"  话题 '{topic_name}' 类型: {msgtype}")
        
        print(f"正在提取话题 '{topic_name}' 的数据...")
        
        # 根据消息类型选择处理方法
        if msgtype == 'sensor_msgs/msg/Image' or msgtype == 'sensor_msgs/Image':
            extract_image_data(typestore, reader, connections, output_dir, topic_name, is_ros1=True)
        else:
            extract_generic_data(typestore, reader, connections, output_dir, topic_name, is_ros1=True)
        
        reader.close()
        return
    except Exception as e:
        if reader:
            try:
                reader.close()
            except:
                pass
        print(f"处理 ROS1 bag 时出错: {e}")
        
    try:
        # 尝试 ROS2 bag 格式
        reader = Bag2Reader(bag_path)
        reader.open()
        # 获取类型存储以反序列化消息
        typestore = get_typestore(Stores.ROS2_FOXY)
        
        # 查找指定话题的连接
        connections = [x for x in reader.connections if x.topic == topic_name]
        if not connections:
            print(f"在 bag 文件中未找到话题 '{topic_name}'")
            reader.close()
            return
            
        # 打印话题类型用于调试
        msgtype = connections[0].msgtype
        print(f"  话题 '{topic_name}' 类型: {msgtype}")
            
        print(f"正在提取话题 '{topic_name}' 的数据...")
        
        # 根据消息类型选择处理方法
        if msgtype == 'sensor_msgs/msg/Image' or msgtype == 'sensor_msgs/Image':
            extract_image_data(typestore, reader, connections, output_dir, topic_name, is_ros1=False)
        else:
            extract_generic_data(typestore, reader, connections, output_dir, topic_name, is_ros1=False)
        
        reader.close()
        return
    except Exception as e:
        if reader:
            try:
                reader.close()
            except:
                pass
        print(f"处理 ROS2 bag 时出错: {e}")


def batch_process_bags(input_dir: str, output_dir: str) -> None:
    """
    批量处理目录中的所有 bag 文件，并将提取的数据保存到指定目录。
    
    参数:
        input_dir: 包含 bag 文件的输入目录
        output_dir: 输出数据的目录
    """
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 获取所有 bag 文件
    bag_files = []
    for file in Path(input_dir).iterdir():
        if file.is_file() and (file.suffix == '.bag'):
            bag_files.append(file)
    
    if not bag_files:
        print(f"在目录 '{input_dir}' 中未找到任何 bag 文件")
        return
    
    print(f"找到 {len(bag_files)} 个 bag 文件")
    
    # 处理每个 bag 文件
    for bag_file in bag_files:
        print(f"\n正在处理: {bag_file.name}")
        bag_path = str(bag_file)
        
        try:
            # 为每个 bag 文件创建单独的输出目录
            bag_output_dir = os.path.join(output_dir, bag_file.stem)
            Path(bag_output_dir).mkdir(parents=True, exist_ok=True)
            
            # 打开 bag 文件并列出所有话题
            topics = []
            reader = None
            try:
                reader = Bag1Reader(bag_path)
                reader.open()
                topics = [(conn.topic, conn.msgtype) for conn in reader.connections]
                reader.close()
            except:
                try:
                    reader = Bag2Reader(bag_path)
                    reader.open()
                    topics = [(conn.topic, conn.msgtype) for conn in reader.connections]
                    reader.close()
                except Exception as e:
                    if reader:
                        reader.close()
                    print(f"无法读取文件 {bag_file.name}: {e}")
                    continue
            
            print(f"  找到 {len(topics)} 个话题")
            
            # 提取每个话题的数据
            for topic_name, topic_type in topics:
                try:
                    print(f"  正在提取话题: {topic_name}")
                    extract_topic_data(bag_path, topic_name, bag_output_dir)
                except Exception as e:
                    print(f"    提取话题 {topic_name} 时出错: {e}")
                    
        except Exception as e:
            print(f"处理文件 {bag_file.name} 时出错: {e}")
    
    print(f"\n所有 bag 文件处理完成，结果保存在: {output_dir}")


def main():
    """
    主函数，解析命令行参数并处理 bag 文件。
    """
    parser = argparse.ArgumentParser(description="解析 ROS bag 文件，为 SUSTechPOINTS 标注工具准备数据")
    parser.add_argument("bag_file", nargs='?', help="ROS bag 文件路径")
    parser.add_argument("--list-topics", action="store_true", 
                        help="列出 bag 文件中的所有话题")
    parser.add_argument("--extract-topic", metavar="TOPIC", 
                        help="提取指定话题的数据")
    parser.add_argument("--output-dir", default="./data/pool", 
                        help="提取数据的输出目录 (默认: ./data/pool)")
    parser.add_argument("--batch-process", action="store_true",
                        help="批量处理 data/bags 目录中的所有 bag 文件")
    
    args = parser.parse_args()
    
    # 批量处理模式
    if args.batch_process:
        batch_process_bags("./data/bags", "./data/pool")
        return
    
    # 检查是否提供了 bag 文件
    if not args.bag_file:
        print("错误：请提供 bag 文件路径或使用 --batch-process 选项")
        return
    
    # 检查 bag 文件是否存在
    if not os.path.exists(args.bag_file):
        print(f"错误：bag 文件 '{args.bag_file}' 不存在")
        return
    
    # 根据参数进行处理
    if args.list_topics:
        list_topics(args.bag_file)
    elif args.extract_topic:
        extract_topic_data(args.bag_file, args.extract_topic, args.output_dir)
    else:
        # 默认操作：列出话题
        list_topics(args.bag_file)


if __name__ == "__main__":
    main()