CREATE USER IF NOT EXISTS spark_user IDENTIFIED BY 'YourSecurePassword';

GRANT ALL PRIVILEGES ON default.* TO spark_user;

GRANT CREATE TEMPORARY TABLE, FILE ON *.* TO spark_user;

CREATE TABLE IF NOT EXISTS default.iot_data (
    uid String,
    id_orig_h String,
    id_orig_p Nullable(Int32),
    id_resp_h String,
    id_resp_p Nullable(Int32),
    proto String,
    service Nullable(String),
    orig_bytes Nullable(Int64),
    resp_bytes Nullable(Int64),
    conn_state String,
    missed_bytes Nullable(Int64),
    history String,
    orig_pkts Nullable(Int64),
    orig_ip_bytes Nullable(Int64),
    resp_pkts Nullable(Int64),
    resp_ip_bytes Nullable(Int64),
    tunnel_parents String,
    label String,
    detailed_label String,
    duration_sec Nullable(Float64),
    local_orig_bool Nullable(UInt8),
    local_resp_bool Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY uid;