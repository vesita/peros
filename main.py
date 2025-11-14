#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# 添加脚本目录到Python路径
script_dir = Path(__file__).parent / 'scripts'
sys.path.insert(0, str(script_dir))

try:
    from scripts.bag_processor import process_bag_files
    IMPORT_SUCCESS = True
except ImportError as e:
    print(f"无法导入bag处理器: {e}")
    IMPORT_SUCCESS = False

def main():
    print("PeROS解析工具")
    print("==============")
    
    # 确保数据目录存在
    data_dir = Path('./data')
    data_dir.mkdir(exist_ok=True)
    
    # 检查是否有bag文件需要处理
    bags_dir = data_dir / 'bags'
    if bags_dir.exists() and any(bags_dir.iterdir()):
        print("发现待处理的bag文件，正在处理...")
        # 导入并运行bag处理器
        if IMPORT_SUCCESS:
            process_bag_files('./data/bags', './data')
        else:
            print("由于缺少必要的模块，无法处理bag文件。")
            return
    else:
        print("未发现bag文件")

if __name__ == "__main__":
    main()