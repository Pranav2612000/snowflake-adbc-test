use adbc_core::constants::ADBC_INGEST_OPTION_MODE_CREATE_APPEND;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

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

    // Create initial schema with two fields: id and name
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // Create first batch of data
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("Failed to create record batch");

    println!("First batch created with {} rows", batch1.num_rows());

    // Perform bulk insert with CREATE_APPEND mode
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Set the target table name
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("test_table".to_string()),
    )
    .expect("Failed to set target table");

    // Set ingest mode to CREATE_APPEND
    stmt.set_option(
        OptionStatement::IngestMode,
        OptionValue::String(ADBC_INGEST_OPTION_MODE_CREATE_APPEND.to_string()),
    )
    .expect("Failed to set ingest mode");

    // Bind and execute the bulk insert
    stmt.bind(batch1).expect("Failed to bind batch");
    stmt.execute_update()
        .expect("Failed to execute bulk insert");

    println!("First bulk insert completed");

    // Create extended schema with an additional field: age
    let extended_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]);

    // Create second batch with the extra field
    let id_array2 = Int32Array::from(vec![4, 5]);
    let name_array2 = StringArray::from(vec!["David", "Eve"]);
    let age_array = Int32Array::from(vec![30, 25]);

    let batch2 = RecordBatch::try_new(
        Arc::new(extended_schema),
        vec![
            Arc::new(id_array2),
            Arc::new(name_array2),
            Arc::new(age_array),
        ],
    )
    .expect("Failed to create second record batch");

    println!(
        "Second batch created with {} rows and {} columns",
        batch2.num_rows(),
        batch2.num_columns()
    );

    // Create a new statement for the second insert
    let mut stmt2 = conn.new_statement().expect("Failed to create statement");

    stmt2
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("test_table".to_string()),
        )
        .expect("Failed to set target table");

    stmt2
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String(ADBC_INGEST_OPTION_MODE_CREATE_APPEND.to_string()),
        )
        .expect("Failed to set ingest mode");

    // Bind and execute the second bulk insert with extra field
    stmt2.bind(batch2).expect("Failed to bind second batch");
    stmt2
        .execute_update()
        .expect("Failed to execute second bulk insert");

    println!("Second bulk insert completed with extra field 'age'");
    println!("All operations completed successfully!");
}

#[test]
fn test_driver_setup() {
    const SNOWFLAKE_DRIVER_NAME: &str = "snowflake";
    let mut driver = ManagedDriver::load_from_name(
        SNOWFLAKE_DRIVER_NAME,
        None,
        AdbcVersion::default(),
        LOAD_FLAG_DEFAULT,
        None,
    )
    .unwrap();
    println!("Driver loaded");
}
