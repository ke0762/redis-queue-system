# Redis 門市叫號系統

## 一、專題介紹

本專題使用 Python、FastAPI 與 Redis，開發一套網頁式門市叫號系統。

透過 Redis 儲存顧客取號資訊、管理等候佇列，並記錄叫號事件，模擬實際門市的取號與叫號流程。

## 二、專題功能

### 1. 顧客取號

- 顧客可以透過網頁取得號碼。
- 系統自動產生取號編號。
- 顯示目前服務號碼及等候人數。

### 2. 櫃台叫號

- 工作人員可以操作叫下一號。
- 顯示目前服務中的號碼。
- 完成服務後，可以繼續叫號。

### 3. 事件紀錄

- 記錄顧客取號、叫號等事件。
- 顯示最近的系統操作紀錄。

## 三、使用技術

| 技術 | 用途 |
|---|---|
| Python | 系統主要程式語言 |
| FastAPI | 建立網頁及 API |
| Redis | 儲存及管理叫號資料 |
| Uvicorn | 執行 FastAPI 網頁伺服器 |
| Ubuntu | 系統執行環境 |
| Cloudflare Tunnel | 提供外部網路存取測試 |

## 四、Redis 技術應用

本專題規劃使用以下 Redis 功能：

### Sorted Set

管理顧客等候佇列，依照取號順序進行叫號。

相關指令：

- ZADD
- ZPOPMIN
- ZRANGE
- ZCARD

### Streams

儲存取號、叫號與服務完成等事件紀錄。

相關指令：

- XADD
- XREVRANGE
- XLEN

### Pub/Sub

發布系統更新訊息，協助通知網頁狀態變化。

相關指令：

- PUBLISH
- SUBSCRIBE

### Lua Script

將需要一起完成的 Redis 操作整合執行，降低並行操作造成資料不一致的風險。

## 五、專題檔案

```text
redis-queue/
├── main.py
├── requirements.txt
└── README.md
```

- `main.py`：門市叫號系統主程式。
- `requirements.txt`：Python 套件需求清單。
- `README.md`：專題說明文件。

## 六、安裝與執行

### 1. 安裝 Redis Server

在 Ubuntu 執行：

```bash
sudo apt update
sudo apt install redis-server
```

### 2. 安裝 Python 套件

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

### 3. 啟動 Redis

```bash
sudo systemctl start redis-server
```

### 4. 啟動門市叫號系統

```bash
.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

### 5. 開啟網頁

顧客取號頁面：

`http://127.0.0.1:8000/`

櫃台叫號頁面：

`http://127.0.0.1:8000/counter`

## 七、操作流程

1. 顧客進入取號頁面。
2. 點擊「我要取號」取得號碼。
3. 系統將號碼加入 Redis 等候佇列。
4. 工作人員進入櫃台頁面。
5. 點擊「叫下一號」。
6. 系統更新目前服務號碼。
7. 完成服務後，繼續處理下一位顧客。

## 八、專題成果

本專題完成基本的網頁取號與櫃台叫號功能，並利用 Redis 管理等候資料與事件紀錄。

透過實作，了解 Redis 除了快取之外，也能應用於佇列管理、事件紀錄與即時訊息通知等情境。

