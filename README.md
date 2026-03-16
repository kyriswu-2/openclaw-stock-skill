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

默认公共服务地址为 `https://akshare.devtool.uk`。面向普通用户时，直接使用默认配置即可，不需要自建服务，也不需要额外安装 `akshare`、`pandas` 或 `docker`。

## Skill 使用说明

本仓库已经提供 [SKILL.md](SKILL.md)，skill 名称为 `akshare-api`，可直接使用。

- skill 内部固定调用 `python3 main.py --query "<自然语言问题>"`
- 服务端地址固定走公共服务 `https://akshare.devtool.uk`
- 用户只需要像聊天一样输入问题，不需要关心服务部署

性能建议：如果用户已经给出明确代码（如 `600519`、`AAPL`、`BRK.A`、`HK00700`），优先保留代码执行。服务端会跳过全量名称列表匹配，减少不必要的网络请求。

## 自建服务代理注意事项

- `AKSHARE_PROXY_URL` 推荐使用 `http://...` 代理地址（当前仓库默认也是 http）。
- 如果使用 `socks5://...`，需要额外安装 SOCKS 依赖（例如 `requests[socks]`/`PySocks`），否则会出现 `Missing dependencies for SOCKS support`。

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
