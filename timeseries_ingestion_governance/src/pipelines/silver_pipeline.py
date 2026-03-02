import dlt
from pyspark.sql import functions as F

# Passed from resources/pipelines.yml configuration
CATALOG = spark.conf.get("iot.catalog")
BRONZE_SCHEMA = spark.conf.get("iot.bronze_schema")


def bronze_table(name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{name}"


def component_key_from_path(path_col):
    # Stable deterministic component key
    return F.sha2(path_col, 256)


def add_uns_from_path(df):
    # Derive UNS once from Path (avoids ambiguous columns across joins)
    return (
        df.withColumn("uns_enterprise", F.split(F.col("Path"), "/").getItem(0))
          .withColumn("uns_site",       F.split(F.col("Path"), "/").getItem(1))
          .withColumn("uns_area",       F.split(F.col("Path"), "/").getItem(2))
          .withColumn("uns_machine",    F.split(F.col("Path"), "/").getItem(3))
          .withColumn("uns_component",  F.split(F.col("Path"), "/").getItem(4))
    )


# -------------------------------
# SILVER DIMENSION: dim_component
# -------------------------------
@dlt.table(
    name="dim_component",
    comment="Silver dimension: one row per component Path, with sensor_ids/installation_dates across measurement types."
)
@dlt.expect_all({
    "path_not_null": "Path IS NOT NULL",
    "component_key_not_null": "component_key IS NOT NULL"
})
def dim_component():
    # Select only non-ambiguous columns from each component table
    p = (
        spark.read.table(bronze_table("power_components"))
        .select("Path", "sensor_id", "installation_date", "name")
        .withColumnRenamed("sensor_id", "sensor_id_power")
        .withColumnRenamed("installation_date", "installation_date_power")
        .withColumnRenamed("name", "name_power")
    )

    s = (
        spark.read.table(bronze_table("speed_components"))
        .select("Path", "sensor_id", "installation_date", "name")
        .withColumnRenamed("sensor_id", "sensor_id_speed")
        .withColumnRenamed("installation_date", "installation_date_speed")
        .withColumnRenamed("name", "name_speed")
    )

    t = (
        spark.read.table(bronze_table("temp_components"))
        .select("Path", "sensor_id", "installation_date", "name")
        .withColumnRenamed("sensor_id", "sensor_id_temp")
        .withColumnRenamed("installation_date", "installation_date_temp")
        .withColumnRenamed("name", "name_temp")
    )

    v = (
        spark.read.table(bronze_table("vibration_components"))
        .select("Path", "sensor_id", "installation_date", "name")
        .withColumnRenamed("sensor_id", "sensor_id_vibration")
        .withColumnRenamed("installation_date", "installation_date_vibration")
        .withColumnRenamed("name", "name_vibration")
    )

    # Base set of unique Paths across all component tables
    paths = (
        p.select("Path")
        .unionByName(s.select("Path"))
        .unionByName(t.select("Path"))
        .unionByName(v.select("Path"))
        .dropDuplicates(["Path"])
    )

    dim = (
        paths
        .join(p, on="Path", how="left")
        .join(s, on="Path", how="left")
        .join(t, on="Path", how="left")
        .join(v, on="Path", how="left")
        .withColumn("component_key", component_key_from_path(F.col("Path")))
        .withColumn("component_name", F.coalesce("name_power", "name_speed", "name_temp", "name_vibration"))
        .drop("name_power", "name_speed", "name_temp", "name_vibration")
    )

    # Derive UNS fields once from Path (consistent + no ambiguity)
    dim = add_uns_from_path(dim)

    return dim


# -----------------------------------
# SILVER FACT: fact_measurement_long
# -----------------------------------
@dlt.table(
    name="fact_measurement_long",
    comment="Silver fact (long format): unified measurements across speed/temp/vibration/power joined to component_key via sensor_id."
)
@dlt.expect_all({
    "event_ts_not_null": "event_ts IS NOT NULL",
    "component_key_not_null": "component_key IS NOT NULL",
    "measurement_not_null": "measurement IS NOT NULL"
})
def fact_measurement_long():
    # Static mapping tables: sensor_id -> component_key
    speed_map = (
        spark.read.table(bronze_table("speed_components"))
        .select("sensor_id", "Path")
        .withColumn("component_key", component_key_from_path(F.col("Path")))
        .select("sensor_id", "component_key")
    )

    temp_map = (
        spark.read.table(bronze_table("temp_components"))
        .select("sensor_id", "Path")
        .withColumn("component_key", component_key_from_path(F.col("Path")))
        .select("sensor_id", "component_key")
    )

    vib_map = (
        spark.read.table(bronze_table("vibration_components"))
        .select("sensor_id", "Path")
        .withColumn("component_key", component_key_from_path(F.col("Path")))
        .select("sensor_id", "component_key")
    )

    power_map = (
        spark.read.table(bronze_table("power_components"))
        .select("sensor_id", "Path")
        .withColumn("component_key", component_key_from_path(F.col("Path")))
        .select("sensor_id", "component_key")
    )

    # SPEED -> long
    speed = (
        spark.readStream.table(bronze_table("speed_readings"))
        .select("sensor_id", "timestamp", "hertz", "_ingest_date", "_source_file")
        .join(speed_map, on="sensor_id", how="left")
        .select(
            F.col("timestamp").alias("event_ts"),
            F.col("component_key"),
            F.col("sensor_id"),
            F.lit("speed_hz").alias("measurement"),
            F.col("hertz").cast("double").alias("value_double"),
            F.lit(None).cast("string").alias("value_string"),
            F.lit("Hz").alias("unit"),
            F.col("_ingest_date"),
            F.col("_source_file")
        )
    )

    # TEMP -> long (use canonical degrees_c produced in bronze)
    temp = (
        spark.readStream.table(bronze_table("temp_readings"))
        .select("sensor_id", "timestamp", "degrees_c", "_ingest_date", "_source_file")
        .join(temp_map, on="sensor_id", how="left")
        .select(
            F.col("timestamp").alias("event_ts"),
            F.col("component_key"),
            F.col("sensor_id"),
            F.lit("temp_c").alias("measurement"),
            F.col("degrees_c").cast("double").alias("value_double"),
            F.lit(None).cast("string").alias("value_string"),
            F.lit("C").alias("unit"),
            F.col("_ingest_date"),
            F.col("_source_file")
        )
    )

    # POWER -> long (categorical)
    power = (
        spark.readStream.table(bronze_table("power_readings"))
        .select("sensor_id", "timestamp", "status", "_ingest_date", "_source_file")
        .join(power_map, on="sensor_id", how="left")
        .select(
            F.col("timestamp").alias("event_ts"),
            F.col("component_key"),
            F.col("sensor_id"),
            F.lit("power_state").alias("measurement"),
            F.lit(None).cast("double").alias("value_double"),
            F.col("status").cast("string").alias("value_string"),
            F.lit(None).cast("string").alias("unit"),
            F.col("_ingest_date"),
            F.col("_source_file")
        )
    )

    # VIBRATION -> 3 measures per row to long
    vib_base = (
        spark.readStream.table(bronze_table("vibration_readings"))
        .select("sensor_id", "timestamp", "acceleration", "velocity", "peak_to_peak", "_ingest_date", "_source_file")
        .join(vib_map, on="sensor_id", how="left")
    )

    vib_long = (
        vib_base
        .select(
            F.col("timestamp").alias("event_ts"),
            F.col("component_key"),
            F.col("sensor_id"),
            F.expr(
                "stack(3, "
                "'vibration_acceleration', acceleration, "
                "'vibration_velocity', velocity, "
                "'vibration_peak_to_peak', peak_to_peak"
                ") as (measurement, value_double)"
            ),
            F.col("_ingest_date"),
            F.col("_source_file"),
        )
        .select(
            "event_ts", "component_key", "sensor_id", "measurement",
            F.col("value_double").cast("double").alias("value_double"),
            F.lit(None).cast("string").alias("value_string"),
            F.lit(None).cast("string").alias("unit"),  # set below
            "_ingest_date", "_source_file"
        )
        # ✅ Assign vibration units (source files don't provide units)
        .withColumn(
            "unit",
            F.when(F.col("measurement") == F.lit("vibration_acceleration"), F.lit("m/s^2"))
             .when(F.col("measurement") == F.lit("vibration_velocity"), F.lit("mm/s"))
             .when(F.col("measurement") == F.lit("vibration_peak_to_peak"), F.lit("mm"))
             .otherwise(F.lit(None))
        )
    )

    fact = speed.unionByName(temp).unionByName(power).unionByName(vib_long)

    # Keep only rows where mapping succeeded
    return fact.where(F.col("event_ts").isNotNull() & F.col("component_key").isNotNull())
