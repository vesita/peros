# PeROS - ROS包到SUSTechPOINTS文件结构解析工具

PeROS是一个专门用于将ROS bag文件中的点云和图像数据转换为SUSTechPOINTS标注工具所需文件结构的解析工具。

## 功能特点

- 自动解析ROS1和ROS2 bag文件
- 提取激光雷达点云数据并转换为PCD格式
- 提取图像数据并转换为JPEG格式
- 自动生成符合SUSTechPOINTS标注工具要求的目录结构
- 提供Web界面进行数据标注

## 安装依赖

### 使用 uv (推荐)

[uv](https://docs.astral.sh/uv/) 是一个超快的 Python 包管理器，兼容 pip 和 venv。

```bash
# 创建 Python 3.12 虚拟环境
uv venv --python 3.12

# 激活虚拟环境
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\Activate.ps1  # Windows PowerShell

# 安装项目依赖
uv sync
```

### 传统方式

```bash
pip install -r requirements.txt
```

或者直接安装:

```bash
pip install rosbags pillow tqdm
```

## 使用方法

### 1. 准备数据

将您的ROS bag文件放置在 `data/bags` 目录中:

```bash
mkdir -p data/bags
cp your_ros_files/*.bag data/bags/
```

### 2. 运行处理脚本

```bash
python main.py
```

这将自动处理 `data/bags` 目录中的所有bag文件，并生成如下目录结构:

```
data/
├── bag_filename/              # 每个bag文件对应一个场景目录
│   ├── lidar/                 # 激光雷达点云数据 (.pcd文件)
│   │   ├── 000000.pcd
│   │   └── ...
│   ├── camera/                # 相机数据目录
│   │   ├── image/             # 图像数据，按统一序号命名
│   │   │   ├── 000000.jpg
│   │   │   └── ...
│   │   └── ...
│   ├── label/                 # 标注结果目录
│   └── desc.json              # 场景描述文件
└── bags/                      # 原始bag文件目录
    ├── your_file1.bag
    └── your_file2.bag
```

### 3. 开始标注

处理完成后，系统会提示您在浏览器中访问 [http://localhost:8080](http://localhost:8080) 进行标注。

## 数据格式说明

### 点云数据

点云数据会被转换为PCD格式，文件名使用统一序号命名，例如: `000000.pcd`

### 图像数据

图像数据会被转换为JPEG格式，同样使用统一序号命名，存储在`camera/image/`目录中，例如: `000000.jpg`

### 时间戳格式

所有文件均使用统一序号命名，格式为: `{index}`，确保匹配的点云和图像文件具有相同的文件名（除了扩展名）。

## 注意事项

- IMU数据会被忽略，仅处理点云和图像数据
- 文件名基于时间戳自动生成，确保数据同步
- 所有处理脚本位于 `scripts/` 目录中，不会暴露在项目根目录
- 处理后的数据可直接用于SUSTechPOINTS标注工具
- 匹配的点云和图像文件具有完全相同的文件名（除了扩展名.pcd和.jpg）
