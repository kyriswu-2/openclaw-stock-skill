FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（fontconfig 用于 matplotlib 中文字体支持）
RUN apt-get update && apt-get install -y --no-install-recommends \
    fontconfig \
    fonts-wqy-zenhei \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝 adapters/（含 AkshareAdapter）
COPY adapters/ /app/adapters/

# 拷贝服务入口
COPY service/main.py /app/main.py

# 预建 matplotlib 字体缓存
RUN python -c "import matplotlib; matplotlib.font_manager._load_fontmanager()" || true

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]