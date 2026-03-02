-- Top components by anomaly count
SELECT
  uns_machine,
  uns_component,
  anomaly_count,
  latest_anomaly_time,
  anomaly_reasons,
  recommendation
FROM main.iot_gold.maintenance_recommendations
ORDER BY anomaly_count DESC
LIMIT 20;

-- Correlation-ready dataset sample
SELECT
  time_bucket,
  uns_machine,
  uns_component,
  avg_speed_hz,
  avg_temp_c,
  avg_vibration_velocity
FROM main.iot_gold.component_health_1m
ORDER BY time_bucket DESC
LIMIT 50;
