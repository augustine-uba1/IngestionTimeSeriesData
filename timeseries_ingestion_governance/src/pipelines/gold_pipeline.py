import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("iot.catalog")
SILVER_SCHEMA = spark.conf.get("iot.silver_schema")

def silver_table(name: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"

# -------------------------------
# GOLD 1: Correlated dataset (1m)
# -------------------------------
@dlt.table(
    name="component_health_1m",
    comment="Gold: 1-minute correlated health dataset per component (speed/temp/vibration aligned)."
)
@dlt.expect_all({
    "time_bucket_not_null": "time_bucket IS NOT NULL",
    "component_key_not_null": "component_key IS NOT NULL"
})
def component_health_1m():
    fact = spark.read.table(silver_table("fact_measurement_long"))
    dim = spark.read.table(silver_table("dim_component"))

    fact_f = fact.filter(
        F.col("measurement").isin(
            "speed_hz", "temp_c",
            "vibration_velocity", "vibration_acceleration", "vibration_peak_to_peak",
            "power_state"
        )
    )

    fact_b = fact_f.withColumn("time_bucket", F.date_trunc("minute", F.col("event_ts")))

    agg = (
        fact_b.groupBy("time_bucket", "component_key")
        .agg(
            F.avg(F.when(F.col("measurement") == "speed_hz", F.col("value_double"))).alias("avg_speed_hz"),
            F.avg(F.when(F.col("measurement") == "temp_c", F.col("value_double"))).alias("avg_temp_c"),
            F.avg(F.when(F.col("measurement") == "vibration_velocity", F.col("value_double"))).alias("avg_vibration_velocity"),
            F.avg(F.when(F.col("measurement") == "vibration_acceleration", F.col("value_double"))).alias("avg_vibration_acceleration"),
            F.avg(F.when(F.col("measurement") == "vibration_peak_to_peak", F.col("value_double"))).alias("avg_vibration_peak_to_peak"),
            F.avg(
                F.when(
                    (F.col("measurement") == "power_state") & (F.col("value_string") == "ON"),
                    F.lit(1.0)
                ).when(
                    (F.col("measurement") == "power_state") & (F.col("value_string") == "OFF"),
                    F.lit(0.0)
                )
            ).alias("power_on_ratio")
        )
    )

    out = (
        agg.join(
            dim.select(
                "component_key", "Path", "component_name",
                "uns_enterprise", "uns_site", "uns_area", "uns_machine", "uns_component"
            ),
            on="component_key",
            how="left"
        )
    )

    return out


# ------------------------------------
# GOLD 2: Component statistics (helper)
# ------------------------------------
@dlt.table(
    name="component_stats",
    comment="Gold helper: per-component mean/std for core measures (used for anomaly detection)."
)
def component_stats():
    h = dlt.read("component_health_1m")

    return (
        h.groupBy("component_key")
         .agg(
            F.avg("avg_speed_hz").alias("mean_speed_hz"),
            F.stddev_pop("avg_speed_hz").alias("std_speed_hz"),

            F.avg("avg_temp_c").alias("mean_temp_c"),
            F.stddev_pop("avg_temp_c").alias("std_temp_c"),

            F.avg("avg_vibration_velocity").alias("mean_vibration_velocity"),
            F.stddev_pop("avg_vibration_velocity").alias("std_vibration_velocity"),
        )
    )


# -------------------------------
# GOLD 3: Anomalies
# -------------------------------
@dlt.table(
    name="component_anomalies",
    comment="Gold: anomaly rows flagged using simple z-score thresholds per component."
)
def component_anomalies():
    h = dlt.read("component_health_1m")
    s = dlt.read("component_stats")

    df = h.join(s, on="component_key", how="left")

    def zscore(val_col, mean_col, std_col):
        return (
            F.when((F.col(std_col).isNull()) | (F.col(std_col) == 0), F.lit(None))
             .otherwise((F.col(val_col) - F.col(mean_col)) / F.col(std_col))
        )

    df = (
        df.withColumn("z_temp", zscore("avg_temp_c", "mean_temp_c", "std_temp_c"))
          .withColumn("z_vibration_velocity", zscore("avg_vibration_velocity", "mean_vibration_velocity", "std_vibration_velocity"))
    )

    # Thresholds (you tuned vibration to 1 for this dataset)
    df = df.withColumn(
        "anomaly_flag",
        (F.col("z_temp") > 3) | (F.col("z_vibration_velocity") > 1)
    )

    # ✅ Robust anomaly_reason creation (no null arrays)
    reasons_raw = F.array(
        F.when(F.col("z_temp") > 3, F.lit("TEMP_SPIKE_VS_BASELINE")),
        F.when(F.col("z_vibration_velocity") > 1, F.lit("VIBRATION_SPIKE_VS_BASELINE"))
    )

    df = df.withColumn("_reasons_tmp", reasons_raw)
    df = df.withColumn("anomaly_reason", F.expr("filter(_reasons_tmp, x -> x is not null)")).drop("_reasons_tmp")

    return df.filter(F.col("anomaly_flag") == True)


# ---------------------------------
# GOLD 4: Maintenance recommendations
# ---------------------------------
@dlt.table(
    name="maintenance_recommendations",
    comment="Gold: simple data-driven maintenance recommendations per component based on anomaly patterns."
)
def maintenance_recommendations():
    anomalies = dlt.read("component_anomalies")

    exploded = (
        anomalies
        .select(
            "component_key", "Path", "component_name",
            "uns_enterprise", "uns_site", "uns_area", "uns_machine", "uns_component",
            "time_bucket",
            F.explode_outer("anomaly_reason").alias("reason")
        )
    )

    agg = (
        exploded.groupBy(
            "component_key", "Path", "component_name",
            "uns_enterprise", "uns_site", "uns_area", "uns_machine", "uns_component"
        )
        .agg(
            F.countDistinct("time_bucket").alias("anomaly_count"),
            F.max("time_bucket").alias("latest_anomaly_time"),
            F.array_remove(F.collect_set("reason"), F.lit(None)).alias("anomaly_reasons")
        )
    )

    out = agg.withColumn(
        "recommendation",
        F.when(
            F.array_contains(F.col("anomaly_reasons"), "VIBRATION_SPIKE_VS_BASELINE") &
            F.array_contains(F.col("anomaly_reasons"), "TEMP_SPIKE_VS_BASELINE"),
            F.lit("Inspect bearings/mountings and check lubrication; vibration + temperature spikes vs baseline.")
        ).when(
            F.array_contains(F.col("anomaly_reasons"), "VIBRATION_SPIKE_VS_BASELINE"),
            F.lit("Inspect bearings/mountings; vibration spike vs baseline detected.")
        ).when(
            F.array_contains(F.col("anomaly_reasons"), "TEMP_SPIKE_VS_BASELINE"),
            F.lit("Check lubrication/friction points; temperature spike vs baseline detected.")
        ).otherwise(
            F.lit("Review component telemetry; anomaly detected.")
        )
    )

    return out
