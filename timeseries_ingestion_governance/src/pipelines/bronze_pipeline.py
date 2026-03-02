import dlt
from pyspark.sql import functions as F

# These are passed from your resources/pipelines.yml configuration
CATALOG = spark.conf.get("iot.catalog")
RAW_SCHEMA = spark.conf.get("iot.raw_schema")

DATE_FMT = "dd/MM/yyyy"
TS_FMT = "dd/MM/yyyy HH:mm"

def read_raw_stream(table_name: str):
    """Read a raw Delta table as a stream for DLT."""
    return spark.readStream.table(f"{CATALOG}.{RAW_SCHEMA}.{table_name}")

def parse_common_component_cols(df):
    # Keep if already DATE; otherwise try dd/MM/yyyy then ISO yyyy-MM-dd
    return df.withColumn(
        "installation_date",
        F.coalesce(
            F.col("installation_date").cast("date"),
            F.to_date(F.col("installation_date"), DATE_FMT),
            F.to_date(F.col("installation_date"), "yyyy-MM-dd")
        )
    )

def parse_common_reading_cols(df):
    # Keep if already TIMESTAMP; otherwise try dd/MM then ISO formats
    ts = F.col("timestamp")
    return df.withColumn(
        "timestamp",
        F.coalesce(
            ts.cast("timestamp"),
            F.to_timestamp(ts, TS_FMT),
            F.to_timestamp(ts, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(ts, "yyyy-MM-dd HH:mm")
        )
    )

def add_uns_from_path(df):
    """
    Path looks like:
    intelliam_ale/site2/line05/Bottle_Filler/Filler_Head_01
    """
    return (
        df
        .withColumn("uns_enterprise", F.split(F.col("Path"), "/").getItem(0))
        .withColumn("uns_site",       F.split(F.col("Path"), "/").getItem(1))
        .withColumn("uns_area",       F.split(F.col("Path"), "/").getItem(2))
        .withColumn("uns_machine",    F.split(F.col("Path"), "/").getItem(3))
        .withColumn("uns_component",  F.split(F.col("Path"), "/").getItem(4))
    )

def with_quality_flags(df, checks):
    """
    checks: list of tuples -> (flag_col_name, boolean_expr, failure_message)
    Adds:
      - each flag col
      - _is_valid
      - _quarantine_reason (array of failure messages)
    """
    for flag_name, expr, _msg in checks:
        df = df.withColumn(flag_name, expr)

    is_valid_expr = None
    for flag_name, _expr, _msg in checks:
        is_valid_expr = F.col(flag_name) if is_valid_expr is None else (is_valid_expr & F.col(flag_name))
    df = df.withColumn("_is_valid", is_valid_expr)

    reason_array = F.array(*[
        F.when(~F.col(flag_name), F.lit(msg)).otherwise(F.lit(None))
        for flag_name, _expr, msg in checks
    ])
    df = df.withColumn("_quarantine_reason", F.array_remove(reason_array, F.lit(None)))

    return df

# ============================================================
# COMPONENTS (Bronze)
# ============================================================

@dlt.table(
    name="power_components",
    comment="Bronze: power sensor components with UNS breakdown and parsed installation_date."
)
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "path_not_null": "Path IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "installation_date_parsed": "installation_date IS NOT NULL",
    "name_not_null": "name IS NOT NULL"
})
def power_components():
    df = read_raw_stream("power_components_raw")
    df = parse_common_component_cols(df)
    df = add_uns_from_path(df)
    return df


@dlt.table(
    name="speed_components",
    comment="Bronze: speed sensor components with UNS breakdown and parsed installation_date."
)
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "path_not_null": "Path IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "installation_date_parsed": "installation_date IS NOT NULL",
    "name_not_null": "name IS NOT NULL"
})
def speed_components():
    df = read_raw_stream("speed_components_raw")
    df = parse_common_component_cols(df)
    df = add_uns_from_path(df)
    return df


@dlt.table(
    name="temp_components",
    comment="Bronze: temperature sensor components with UNS breakdown and parsed installation_date."
)
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "path_not_null": "Path IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "installation_date_parsed": "installation_date IS NOT NULL",
    "name_not_null": "name IS NOT NULL"
})
def temp_components():
    df = read_raw_stream("temp_components_raw")
    df = parse_common_component_cols(df)
    df = add_uns_from_path(df)
    return df


@dlt.table(
    name="vibration_components",
    comment="Bronze: vibration sensor components with UNS breakdown and parsed installation_date."
)
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "path_not_null": "Path IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "installation_date_parsed": "installation_date IS NOT NULL",
    "name_not_null": "name IS NOT NULL"
})
def vibration_components():
    df = read_raw_stream("vibration_components_raw")
    df = parse_common_component_cols(df)
    df = add_uns_from_path(df)
    return df


# ============================================================
# READINGS (Bronze + Quarantine)
# ============================================================

def power_checks(df):
    return [
        ("_q_uid_ok",    F.col("UID").isNotNull(),                          "UID is null"),
        ("_q_sid_ok",    F.col("sensor_id").isNotNull(),                    "sensor_id is null"),
        ("_q_ts_ok",     F.col("timestamp").isNotNull(),                    "timestamp parse failed"),
        ("_q_status_ok", F.col("status").isin("ON", "OFF"),                 "status not in ON/OFF"),
    ]

@dlt.table(name="power_readings", comment="Bronze: power readings (ON/OFF) with parsed timestamp. Invalid rows go to power_readings_quarantine.")
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "timestamp_parsed": "timestamp IS NOT NULL",
    "status_is_on_off": "status IN ('ON','OFF')"
})
def power_readings():
    df = read_raw_stream("power_readings_raw")
    df = parse_common_reading_cols(df)
    df = with_quality_flags(df, power_checks(df))
    return df.filter(F.col("_is_valid")).drop("_is_valid")

@dlt.table(name="power_readings_quarantine", comment="Quarantine: invalid power readings with reasons.")
def power_readings_quarantine():
    df = read_raw_stream("power_readings_raw")
    df = parse_common_reading_cols(df)
    df = with_quality_flags(df, power_checks(df))
    return df.filter(~F.col("_is_valid")).drop("_is_valid")


def speed_checks(df):
    return [
        ("_q_uid_ok",   F.col("UID").isNotNull(),                       "UID is null"),
        ("_q_sid_ok",   F.col("sensor_id").isNotNull(),                 "sensor_id is null"),
        ("_q_ts_ok",    F.col("timestamp").isNotNull(),                 "timestamp parse failed"),
        ("_q_hz_ok",    F.col("hertz").cast("double").isNotNull(),       "hertz not numeric"),
        ("_q_hz_rng",   (F.col("hertz").cast("double") >= F.lit(0.0)) &
                        (F.col("hertz").cast("double") <= F.lit(500.0)), "hertz out of range (0-500)"),
    ]

@dlt.table(name="speed_readings", comment="Bronze: speed readings (Hz) with parsed timestamp. Invalid rows go to speed_readings_quarantine.")
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "timestamp_parsed": "timestamp IS NOT NULL",
    "hertz_numeric": "CAST(hertz AS DOUBLE) IS NOT NULL",
    "hertz_range": "CAST(hertz AS DOUBLE) BETWEEN 0 AND 500"
})
def speed_readings():
    df = read_raw_stream("speed_readings_raw")
    df = parse_common_reading_cols(df).withColumn("hertz", F.col("hertz").cast("double"))
    df = with_quality_flags(df, speed_checks(df))
    return df.filter(F.col("_is_valid")).drop("_is_valid")

@dlt.table(name="speed_readings_quarantine", comment="Quarantine: invalid speed readings with reasons.")
def speed_readings_quarantine():
    df = read_raw_stream("speed_readings_raw")
    df = parse_common_reading_cols(df).withColumn("hertz", F.col("hertz").cast("double"))
    df = with_quality_flags(df, speed_checks(df))
    return df.filter(~F.col("_is_valid")).drop("_is_valid")


def temp_checks(df):
    return [
        ("_q_uid_ok",    F.col("UID").isNotNull(),                          "UID is null"),
        ("_q_sid_ok",    F.col("sensor_id").isNotNull(),                    "sensor_id is null"),
        ("_q_ts_ok",     F.col("timestamp").isNotNull(),                    "timestamp parse failed"),
        ("_q_deg_ok",    F.col("degrees").cast("double").isNotNull(),       "degrees not numeric"),
        ("_q_unit_ok",   F.col("unit").isin("C", "F"),                      "unit not in C/F"),
        ("_q_deg_rng",   (F.col("degrees").cast("double") >= F.lit(-50.0)) &
                         (F.col("degrees").cast("double") <= F.lit(300.0)), "degrees out of range (-50..300)"),
    ]

@dlt.table(name="temp_readings", comment="Bronze: temperature readings with parsed timestamp and standardized degrees_c. Invalid rows go to temp_readings_quarantine.")
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "timestamp_parsed": "timestamp IS NOT NULL",
    "degrees_numeric": "CAST(degrees AS DOUBLE) IS NOT NULL",
    "unit_is_c_or_f": "unit IN ('C','F')"
})
def temp_readings():
    df = read_raw_stream("temp_readings_raw")
    df = parse_common_reading_cols(df).withColumn("degrees", F.col("degrees").cast("double"))
    df = df.withColumn(
        "degrees_c",
        F.when(F.col("unit") == F.lit("C"), F.col("degrees"))
         .when(F.col("unit") == F.lit("F"), (F.col("degrees") - F.lit(32.0)) * F.lit(5.0) / F.lit(9.0))
         .otherwise(F.lit(None))
    )
    df = with_quality_flags(df, temp_checks(df))
    return df.filter(F.col("_is_valid")).drop("_is_valid")

@dlt.table(name="temp_readings_quarantine", comment="Quarantine: invalid temperature readings with reasons.")
def temp_readings_quarantine():
    df = read_raw_stream("temp_readings_raw")
    df = parse_common_reading_cols(df).withColumn("degrees", F.col("degrees").cast("double"))
    df = df.withColumn(
        "degrees_c",
        F.when(F.col("unit") == F.lit("C"), F.col("degrees"))
         .when(F.col("unit") == F.lit("F"), (F.col("degrees") - F.lit(32.0)) * F.lit(5.0) / F.lit(9.0))
         .otherwise(F.lit(None))
    )
    df = with_quality_flags(df, temp_checks(df))
    return df.filter(~F.col("_is_valid")).drop("_is_valid")


def vibration_checks(df):
    return [
        ("_q_uid_ok",   F.col("UID").isNotNull(),                             "UID is null"),
        ("_q_sid_ok",   F.col("sensor_id").isNotNull(),                       "sensor_id is null"),
        ("_q_ts_ok",    F.col("timestamp").isNotNull(),                       "timestamp parse failed"),
        ("_q_acc_ok",   F.col("acceleration").cast("double").isNotNull(),      "acceleration not numeric"),
        ("_q_vel_ok",   F.col("velocity").cast("double").isNotNull(),          "velocity not numeric"),
        ("_q_p2p_ok",   F.col("peak_to_peak").cast("double").isNotNull(),      "peak_to_peak not numeric"),
        ("_q_acc_rng",  (F.col("acceleration") >= F.lit(0.0)) & (F.col("acceleration") <= F.lit(100.0)), "acceleration out of range (0-100)"),
        ("_q_vel_rng",  (F.col("velocity") >= F.lit(0.0)) & (F.col("velocity") <= F.lit(100.0)),         "velocity out of range (0-100)"),
        ("_q_p2p_rng",  (F.col("peak_to_peak") >= F.lit(0.0)) & (F.col("peak_to_peak") <= F.lit(1000.0)), "peak_to_peak out of range (0-1000)"),
    ]

@dlt.table(name="vibration_readings", comment="Bronze: vibration readings with parsed timestamp. Invalid rows go to vibration_readings_quarantine.")
@dlt.expect_all({
    "uid_not_null": "UID IS NOT NULL",
    "sensor_id_not_null": "sensor_id IS NOT NULL",
    "timestamp_parsed": "timestamp IS NOT NULL",
    "acc_numeric": "CAST(acceleration AS DOUBLE) IS NOT NULL",
    "vel_numeric": "CAST(velocity AS DOUBLE) IS NOT NULL",
    "p2p_numeric": "CAST(peak_to_peak AS DOUBLE) IS NOT NULL"
})
def vibration_readings():
    df = read_raw_stream("vibration_readings_raw")
    df = parse_common_reading_cols(df)
    df = (
        df.withColumn("acceleration", F.col("acceleration").cast("double"))
          .withColumn("velocity", F.col("velocity").cast("double"))
          .withColumn("peak_to_peak", F.col("peak_to_peak").cast("double"))
    )
    df = with_quality_flags(df, vibration_checks(df))
    return df.filter(F.col("_is_valid")).drop("_is_valid")

@dlt.table(name="vibration_readings_quarantine", comment="Quarantine: invalid vibration readings with reasons.")
def vibration_readings_quarantine():
    df = read_raw_stream("vibration_readings_raw")
    df = parse_common_reading_cols(df)
    df = (
        df.withColumn("acceleration", F.col("acceleration").cast("double"))
          .withColumn("velocity", F.col("velocity").cast("double"))
          .withColumn("peak_to_peak", F.col("peak_to_peak").cast("double"))
    )
    df = with_quality_flags(df, vibration_checks(df))
    return df.filter(~F.col("_is_valid")).drop("_is_valid")