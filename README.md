# OpenClaw Stock Skill

基于 [openclaw](https://github.com/openclaw/openclaw) 的 A 股数据查询 skill。

## 项目架构

本项目拆分为两个独立部分：

### 云端数据服务（`service/`）
- 基于 FastAPI + akshare 构建
- 使用 Docker 部署在云服务器
- 公共服务地址：`https://akshare.devtool.uk`
- 对外提供 `POST /query` 接口，接收意图 JSON，返回股票数据

### 客户端 Skill（`main.py`）
- **普通用户无需安装 akshare、pandas、docker 等任何依赖**
- 只需 Python 3 标准库即可运行
- 通过 HTTP 请求调用云端服务获取数据
- 使用内置 formatter 对数据进行格式化输出

## 快速开始（普通用户）

```bash
# 直接使用公共云端服务，无需任何额外配置
python3 main.py --query "茅台最近30天K线"
python3 main.py --query "今日涨停统计"
python3 main.py --query "上证指数实时行情"
```

## 配置自定义服务地址

三种方式，优先级从高到低：

1. 命令行参数
```bash
python3 main.py --service-url https://your-server.com --query "茅台K线"
```

2. 环境变量
```bash
export AKSHARE_SERVICE_URL=https://your-server.com
python3 main.py --query "茅台K线"
```

3. 默认值（公共服务）：`https://akshare.devtool.uk`

## 自建云端服务

如果你想自己部署数据服务：

```bash
# 在 service/ 目录下构建并运行 Docker 容器
docker build -f service/Dockerfile -t akshare-service .
docker run -d -p 8000:8000 akshare-service
```

然后使用 `--service-url http://localhost:8000` 或设置 `AKSHARE_SERVICE_URL` 指向你的服务。

## 文件说明

| 文件/目录 | 说明 |
|-----------|------|
| `main.py` | 客户端 skill 入口 |
| `router.py` | 意图路由解析（NLP 关键词匹配） |
| `adapters/` | AkshareAdapter，供云端服务使用 |
| `service/main.py` | 云端 FastAPI 服务入口 |
| `service/Dockerfile` | Docker 构建文件 |
| `service/requirements.txt` | 云端服务 Python 依赖 |
| `SKILL.md` | OpenClaw skill 配置文件 |
