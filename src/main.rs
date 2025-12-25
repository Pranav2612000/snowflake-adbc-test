use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, LOAD_FLAG_DEFAULT, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow::util::pretty;
use arrow_array::RecordBatch;

fn main() {
    let mut driver = ManagedDriver::load_from_name(
        "snowflake",
        None,
        AdbcVersion::default(),
        LOAD_FLAG_DEFAULT,
        None,
    )
    .expect("Failed to load driver");

    let opts = [
        (OptionDatabase::Username, "USERNAME".into()),
        (OptionDatabase::Password, "PASSWORD".into()),
        (
            OptionDatabase::Other("adbc.snowflake.sql.auth_type".to_string()),
            "auth_pat".into(),
        ),
        (
            OptionDatabase::Other("adbc.snowflake.sql.account".to_string()),
            "ACCOUNT_ID".into(),
        ),
        (
            OptionDatabase::Other("adbc.snowflake.sql.client_option.auth_token".to_string()),
            "TOKEN".into(),
        ),
        (
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string()),
            "DB_NAME".into(),
        ),
        (
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string()),
            "SCHEMA_NAME".into(),
        ),
    ];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database handle");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let mut statement: adbc_driver_manager::ManagedStatement = conn.new_statement().unwrap();
    statement
        .set_sql_query("MERGE INTO \"sample-flight-data\" AS target USING (SELECT 'pj_fVJsBQRHMz7XyDDY7' AS \"_id\", 'MEL' AS \"OriginAirportID\", 'K5XKPY3' AS \"FlightNum\", 'Melbourne International Airport' AS \"Origin\", '{\"lon\":\"144.843002\",\"lat\":\"-37.673302\"}' AS \"OriginLocation\", '{\"lon\":\"-70.30930328\",\"lat\":\"43.64619827\"}' AS \"DestLocation\", FALSE AS \"FlightDelay\", 10563.139984406853 AS \"DistanceMiles\", 1307.6712273127123 AS \"FlightTimeMin\", 'Cloudy' AS \"OriginWeather\", 5 AS \"dayOfWeek\", 945.9915458470314 AS \"AvgTicketPrice\", 'OpenSearch Dashboards Airlines' AS \"Carrier\", 0 AS \"FlightDelayMin\", 'SE-BD' AS \"OriginRegion\", 'No Delay' AS \"FlightDelayType\", 'PWM' AS \"DestAirportID\", '2025-11-26T08:58:17Z' AS \"timestamp\", 'Portland International Jetport Airport' AS \"Dest\", 21.79452045521187 AS \"FlightTimeHour\", FALSE AS \"Cancelled\", 16999.725955065263 AS \"DistanceKilometers\", 'Melbourne' AS \"OriginCityName\", 'Rain' AS \"DestWeather\", 'AU' AS \"OriginCountry\", 'US' AS \"DestCountry\", 'US-ME' AS \"DestRegion\", 'Portland' AS \"DestCityName\") AS source ON target.\"_id\" = source.\"_id\" WHEN MATCHED THEN UPDATE SET \"OriginAirportID\" = 'MEL', \"FlightNum\" = 'K5XKPY3', \"Origin\" = 'Melbourne International Airport', \"OriginLocation\" = '{\"lon\":\"144.843002\",\"lat\":\"-37.673302\"}', \"DestLocation\" = '{\"lon\":\"-70.30930328\",\"lat\":\"43.64619827\"}', \"FlightDelay\" = FALSE, \"DistanceMiles\" = 10563.139984406853, \"FlightTimeMin\" = 1307.6712273127123, \"OriginWeather\" = 'Cloudy', \"dayOfWeek\" = 5, \"AvgTicketPrice\" = 945.9915458470314, \"Carrier\" = 'OpenSearch Dashboards Airlines', \"FlightDelayMin\" = 0, \"OriginRegion\" = 'SE-BD', \"FlightDelayType\" = 'No Delay', \"DestAirportID\" = 'PWM', \"timestamp\" = '2025-11-26T08:58:17Z', \"Dest\" = 'Portland International Jetport Airport', \"FlightTimeHour\" = 21.79452045521187, \"Cancelled\" = FALSE, \"DistanceKilometers\" = 16999.725955065263, \"OriginCityName\" = 'Melbourne', \"DestWeather\" = 'Rain', \"OriginCountry\" = 'AU', \"DestCountry\" = 'US', \"DestRegion\" = 'US-ME', \"DestCityName\" = 'Portland' WHEN NOT MATCHED THEN INSERT (\"_id\", \"OriginAirportID\", \"FlightNum\", \"Origin\", \"OriginLocation\", \"DestLocation\", \"FlightDelay\", \"DistanceMiles\", \"FlightTimeMin\", \"OriginWeather\", \"dayOfWeek\", \"AvgTicketPrice\", \"Carrier\", \"FlightDelayMin\", \"OriginRegion\", \"FlightDelayType\", \"DestAirportID\", \"timestamp\", \"Dest\", \"FlightTimeHour\", \"Cancelled\", \"DistanceKilometers\", \"OriginCityName\", \"DestWeather\", \"OriginCountry\", \"DestCountry\", \"DestRegion\", \"DestCityName\") VALUES ('aw_fVJsBQRHMz7XyDDY7', 'MEL', 'K5XKPY3', 'Melbourne International Airport', '{\"lon\":\"144.843002\",\"lat\":\"-37.673302\"}', '{\"lon\":\"-70.30930328\",\"lat\":\"43.64619827\"}', FALSE, 10563.139984406853, 1307.6712273127123, 'Cloudy', 5, 945.9915458470314, 'OpenSearch Dashboards Airlines', 0, 'SE-BD', 'No Delay', 'PWM', '2025-11-26T08:58:17Z', 'Portland International Jetport Airport', 21.79452045521187, FALSE, 16999.725955065263, 'Melbourne', 'Rain', 'AU', 'US', 'US-ME', 'Portland');")
        .unwrap();
    println!("Executing statement");
    let reader = statement.execute_update().unwrap();
    println!("Statement executed");
    // let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

    // pretty::print_batches(&batches).expect("Failed to print batches");
}
