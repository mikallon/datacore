#!/usr/bin/env python3
"""
测试本地 LLM Studio API 集成

使用方法：
1. 确保 LM Studio 服务运行在 http://192.168.30.162:1234
2. 设置环境变量（可选）：
   export LLM_PROVIDER="local"
   export LLM_MODEL="your-model-name"

3. 运行测试：
   python3 test_deepseek.py
"""
import os
import sys
sys.path.insert(0, 'web_ui/backend')

from llm_query_parser import LLMQueryParserWithAPI

# 模拟指标和维度
metrics = [
    {'name': 'daily_revenue', 'label': '日收费收入', 'description': '日收费收入'},
    {'name': 'daily_transactions', 'label': '日交易笔数', 'description': '日交易笔数'},
    {'name': 'normal_transaction_rate', 'label': '正常交易率', 'description': '正常交易率'}
]

dimensions = ['city', 'station_name', 'vehicle_type_name']

print("=" * 60)
print("测试本地 LLM Studio API 集成")
print("=" * 60)
print(f"LLM 服务地址: http://192.168.30.162:1234")
print("=" * 60)

try:
    parser = LLMQueryParserWithAPI(
        available_metrics=metrics,
        available_dimensions=dimensions,
        api_key='not-required',  # 本地 LLM Studio 通常不需要 API Key
        model=os.getenv('LLM_MODEL', 'local-model'),
        provider='local'  # 使用本地 LLM Studio
    )
    
    print("\n✅ 本地 LLM Studio 解析器初始化成功")
    
    # 测试查询
    test_query = "查询最近7天的日收入，按城市分组"
    print(f"\n测试查询: {test_query}")
    
    result = parser.parse(test_query)
    
    print("\n✅ 解析成功！")
    print(f"解析结果:")
    print(f"  指标: {result.get('metric_name')}")
    print(f"  维度: {result.get('dimensions')}")
    print(f"  时间范围: {result.get('start_date')} 到 {result.get('end_date')}")
    print(f"  过滤条件: {result.get('filters')}")
    
except Exception as e:
    print(f"\n❌ 测试失败: {e}")
    print("\n提示：")
    print("1. 确保 LM Studio 服务运行在 http://192.168.30.162:1234")
    print("2. 检查网络连接是否正常")
    print("3. 查看 LM Studio 的日志输出")
    import traceback
    traceback.print_exc()
