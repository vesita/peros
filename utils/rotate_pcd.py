#!/usr/bin/env python3
"""
点云旋转工具

将点云数据绕x轴旋转指定角度，并保存到新文件夹中。
该脚本能精确保留原始PCD文件的字段信息（如intensity, ring等）。
"""

import os
import struct
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, List
from tqdm import tqdm


def read_pcd_file(filepath: str) -> Tuple[np.ndarray, Dict[str, str], List[str]]:
    """
    从PCD文件中读取点云数据
    
    参数:
        filepath: PCD文件路径
        
    返回:
        tuple: (points_array, header_info, header_lines)
        - points_array: 点云数据数组，形状为 (N, D)，N为点数，D为维度
        - header_info: PCD文件头信息字典
        - header_lines: PCD文件头原始行列表（不包括DATA行）
    """
    header_info = {}
    header_lines = []
    points = []
    
    with open(filepath, 'r') as f:
        lines = f.readlines()
        
        # 解析头信息
        data_start_idx = 0
        for i, line in enumerate(lines):
            if line.startswith('DATA ascii'):
                data_start_idx = i + 1
                header_lines.append(line.rstrip())
                break
            elif line.startswith('FIELDS') or line.startswith('SIZE') or line.startswith('TYPE') or \
                 line.startswith('COUNT') or line.startswith('WIDTH') or line.startswith('HEIGHT') or \
                 line.startswith('POINTS') or line.startswith('VIEWPOINT'):
                key = line.split()[0]
                value = ' '.join(line.split()[1:])
                header_info[key] = value
                header_lines.append(line.rstrip())
            else:
                header_lines.append(line.rstrip())
        
        # 获取字段信息
        fields = header_info.get('FIELDS', 'x y z').split()
        num_fields = len(fields)

        # 读取点数据
        for line in lines[data_start_idx:]:
            line = line.strip()
            if line and not line.startswith('#'):
                values = line.split()
                # 检查values长度是否至少等于字段数，如果不够，用0填充
                if len(values) >= num_fields:
                    try:
                        point_data = [float(v) for v in values[:num_fields]]
                        points.append(point_data)
                    except ValueError:
                        continue  # 跳过无效行
                elif len(values) > 0:  # 至少有一个值
                    try:
                        # 用0填充缺失的字段
                        point_data = [float(v) for v in values]
                        while len(point_data) < num_fields:
                            point_data.append(0.0)  # 用0填充缺失字段
                        points.append(point_data)
                    except ValueError:
                        continue  # 跳过无效行

    if points:
        return np.array(points), header_info, header_lines
    else:
        return np.empty((0, len(header_info.get('FIELDS', 'x y z').split()))), header_info, header_lines


def write_pcd_file(filepath: str, points: np.ndarray, header_info: Dict[str, str], original_header_lines: List[str]) -> None:
    """
    将点云数据写入PCD文件，保留原始头信息
    
    参数:
        filepath: 输出文件路径
        points: 点云数据数组，形状为 (N, D)，N为点数，D为维度
        header_info: PCD文件头信息
        original_header_lines: 原始头信息行列表
    """
    if points.size == 0:
        return

    num_points, num_fields = points.shape
    
    # 更新必要的头信息
    updated_header_lines = []
    for line in original_header_lines:
        if line.startswith('WIDTH'):
            updated_header_lines.append(f"WIDTH {num_points}")
        elif line.startswith('POINTS'):
            updated_header_lines.append(f"POINTS {num_points}")
        elif line.startswith('HEIGHT'):
            updated_header_lines.append(f"HEIGHT 1")
        else:
            updated_header_lines.append(line)
    
    with open(filepath, 'w') as f:
        # 写入更新后的头信息
        for line in updated_header_lines:
            f.write(line + "\n")
        
        # 写入点云数据
        for point in points:
            f.write(" ".join([f"{val:.6f}" for val in point]) + "\n")


def rotate_pcd_by_negating_yz(points: np.ndarray) -> np.ndarray:
    """
    对点云数据的y,z轴值取反
    
    参数:
        points: 原始点云数据数组，形状为 (N, D)，N为点数，D为维度
        
    返回:
        y,z轴取反后的点云数据数组
    """
    if points.size == 0:
        return points
    
    # 复制点云数据以避免修改原始数据
    result_points = points.copy()
    
    # 对y轴和z轴的值取反（假设点云至少有3个维度：x, y, z）
    if result_points.shape[1] >= 2:  # 有y轴
        result_points[:, 1] = -result_points[:, 1]  # y轴取反
    if result_points.shape[1] >= 3:  # 有z轴
        result_points[:, 2] = -result_points[:, 2]  # z轴取反
    
    return result_points


def process_pcd_files(input_dir: str, output_dir: str) -> None:
    """
    处理目录中的所有PCD文件，绕x轴旋转并保存到新目录
    
    参数:
        input_dir: 包含PCD文件的输入目录
        output_dir: 输出目录
        angle_degrees: 旋转角度（度），默认180度
    """
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 获取所有PCD文件
    pcd_files = []
    for file in Path(input_dir).glob('*.pcd'):
        if file.is_file():
            pcd_files.append(file)
    
    if not pcd_files:
        print(f"在目录 '{input_dir}' 中未找到任何PCD文件")
        return
    
    print(f"找到 {len(pcd_files)} 个PCD文件")
    
    # 处理每个PCD文件
    for pcd_file in tqdm(pcd_files, desc="处理PCD文件"):
        try:
            # 读取原始点云数据
            points, header_info, original_header_lines = read_pcd_file(str(pcd_file))
            
            if points.size == 0:
                print(f"  警告: {pcd_file.name} 中没有点云数据")
                continue
            
            # 对y,z轴值取反
            negated_points = rotate_pcd_by_negating_yz(points)
            
            # 生成输出文件路径
            output_filepath = os.path.join(output_dir, pcd_file.name)
            
            # 保存取反后的点云数据，保留原始头信息
            write_pcd_file(output_filepath, negated_points, header_info, original_header_lines)
            
        except Exception as e:
            print(f"  处理文件 {pcd_file.name} 时出错: {e}")
            continue
    
    print(f"\n所有PCD文件处理完成，结果保存在: {output_dir}")


def process_all_scenes_in_data_output() -> None:
    """
    遍历 data/output 目录下的所有场景，对每个场景的 lidar 目录中的 PCD 文件进行y,z轴取反处理
    并保存到对应场景的 lidar_negated 目录中
    """
    data_output_dir = Path('./data/output')
    
    if not data_output_dir.exists():
        print(f"目录 '{data_output_dir}' 不存在")
        return
    
    # 获取所有场景目录
    scene_dirs = [d for d in data_output_dir.iterdir() if d.is_dir()]
    
    if not scene_dirs:
        print(f"在 '{data_output_dir}' 中未找到任何场景目录")
        return
    
    print(f"找到 {len(scene_dirs)} 个场景目录")
    
    # 遍历每个场景目录
    for scene_dir in scene_dirs:
        lidar_dir = scene_dir / 'lidar'
        
        if not lidar_dir.exists():
            print(f"场景 '{scene_dir.name}' 中没有 lidar 目录，跳过...")
            continue

        # 输出目录为 lidar_totated
        output_dir = scene_dir / 'lidar_totated'
        
        print(f"\n处理场景: {scene_dir.name}")
        process_pcd_files(str(lidar_dir), str(output_dir))


def main():
    
    process_all_scenes_in_data_output()


if __name__ == "__main__":
    main()