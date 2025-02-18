from flask import Flask, request, jsonify
import uuid
from functools import wraps
import threading
from datetime import datetime, timezone
import mt5_trade_analysis
import MetaTrader5 as mt5

app = Flask(__name__)

# 存储任务状态的字典
tasks = {}

# API密钥
API_KEY = "mysecret"

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        if api_key and api_key == API_KEY:
            return f(*args, **kwargs)
        return jsonify({"error": "Invalid API key"}), 401
    return decorated

def run_analysis(task_id):
    try:
        # 连接MT5
        if not mt5.initialize():
            tasks[task_id] = {
                "status": "failed",
                "error": "Failed to connect to MT5",
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            return

        # 获取交易历史
        trades_df, balance_df = mt5_trade_analysis.get_trades_history()
        
        if trades_df is not None:
            # 分析交易数据
            daily_analysis = mt5_trade_analysis.analyze_trades_by_day(trades_df, balance_df)
            
            # 更新任务状态为成功
            tasks[task_id] = {
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
        else:
            tasks[task_id] = {
                "status": "failed",
                "error": "No trade data found",
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        tasks[task_id] = {
            "status": "failed",
            "error": str(e),
            "completed_at": datetime.now(timezone.utc).isoformat()
        }
    finally:
        # 关闭MT5连接
        mt5.shutdown()

@app.route('/analyze', methods=['POST'])
@require_api_key
def start_analysis():
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "running",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # 在新线程中运行分析
    thread = threading.Thread(target=run_analysis, args=(task_id,))
    thread.start()
    
    return jsonify({
        "task_id": task_id,
        "status": "running"
    })

@app.route('/task/<task_id>', methods=['GET'])
@require_api_key
def get_task_status(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 