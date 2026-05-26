from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    has_drift = bool(new_columns or removed_columns or type_changes)

    drift_severity = 'NONE'
    if new_columns:
        if any('null' not in v for v in new_columns.values()):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'

    return {
        'new_columns': new_columns,
       'removed_columns': removed_columns,
        'type_changes': type_changes,
        'drift_severity': drift_severity,
        'has_drift': has_drift
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    decisions = {}
    for column, dtype in drift_report['new_columns'].items():
        if dtype.endswith('null'):
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'nullable new column', 'risk_level': 'LOW'}
        elif column == 'discount_amount':
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'could affect revenue calculations', 'risk_level': 'HIGH'}
        else:
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'new column', 'risk_level': 'LOW'}

    for column, (old_type, new_type) in drift_report['type_changes'].items():
        if old_type!= new_type and 'int' in old_type and 'float' in new_type:
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'type widening', 'risk_level': 'LOW'}
        elif old_type!= new_type and 'float' in old_type and 'int' in new_type:
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'type narrowing', 'risk_level': 'HIGH'}

    for column in drift_report['removed_columns']:
        decisions[column] = {'action': 'HALT','reason':'removed column', 'risk_level': 'BREAKING'}

    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for column, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(column)
            migration_notes.append(f"Column '{column}' silently dropped.")
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Column '{column}' added to schema.")
        elif decision['action'] == 'FLAG_ANOMALY':
            from pyspark.sql.functions import col
            spark_df = spark_df.withColumn(f"{column}_anomaly", col(column).isNull().cast("boolean"))
            migration_notes.append(f"Column '{column}' flagged for anomaly.")

    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    if not drift_report['has_drift']:
        print("No schema drift detected.")
        return drift_report

    decisions = decide_action(drift_report)
    if spark_df:
        spark_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        print("Schema evolution applied successfully.")
        print("Migration notes:")
        for note in migration_notes:
            print(f" - {note}")
    else:
        migration_notes = []

    full_report = {**drift_report, 'decisions': decisions,'migration_notes': migration_notes}
    return full_report
