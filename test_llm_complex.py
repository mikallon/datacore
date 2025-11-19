#!/usr/bin/env python3
"""
测试本地 LLM Studio 的复杂查询解析能力
"""
import os
import sys
sys.path.insert(0, 'web_ui/backend')

from llm_query_parser import LLMQueryParserWithAPI

# 模拟指标和维度
metrics = [
    {'name': 'daily_revenue', 'label': '日收费收入', 'description': '日收费收入'},
    {'name': 'daily_transactions', 'label': '日交易笔数', 'description': '日交易笔数'},
    {'name': 'normal_transaction_rate', 'label': '正常交易率', 'description': '正常交易率'},
    {'name': 'avg_transaction_amount', 'label': '平均交易金额', 'description': '平均交易金额'}
]

dimensions = ['city', 'station_name', 'vehicle_type_name', 'payment_method_name']

print("=" * 60)
print("测试本地 LLM Studio 复杂查询解析")
print("=" * 60)

parser = LLMQueryParserWithAPI(
    available_metrics=metrics,
    available_dimensions=dimensions,
    api_key='not-required',
    model=os.getenv('LLM_MODEL', 'local-model'),
    provider='local'
)

# 测试多个查询
test_queries = [
    "查询最近7天的日收入，按城市分组",
    "查询本月的交易笔数，按收费站分组",
    "查询昨天的正常交易率",
    "查询最近30天的平均交易金额，按城市和车型分组"
]

for i, query in enumerate(test_queries, 1):
    print(f"\n{'='*60}")
    print(f"测试 {i}: {query}")
    print('='*60)
    
    try:
        result = parser.parse(query)
        
        print(f"✅ 解析成功！")
        print(f"  指标: {result.get('metric_name')}")
        print(f"  维度: {result.get('dimensions')}")
        print(f"  时间范围: {result.get('start_date')} 到 {result.get('end_date')}")
        print(f"  过滤条件: {result.get('filters')}")
    except Exception as e:
        print(f"❌ 解析失败: {e}")
        import traceback
        traceback.print_exc()

print(f"\n{'='*60}")
print("测试完成")
print('='*60)

