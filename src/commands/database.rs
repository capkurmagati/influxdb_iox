//! This module implements the `database` CLI command
use std::{fs::File, io::Read, num::NonZeroU64, path::PathBuf, str::FromStr};

use structopt::StructOpt;
use thiserror::Error;

use influxdb_iox_client::{
    connection::Connection,
    flight,
    format::QueryOutputFormat,
    management::{
        self, generated_types::*, CreateDatabaseError, GetDatabaseError, ListDatabaseError,
    },
    write::{self, WriteError},
};

mod chunk;
mod partition;
mod recover;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating database: {0}")]
    CreateDatabaseError(#[from] CreateDatabaseError),

    #[error("Error getting database: {0}")]
    GetDatabaseError(#[from] GetDatabaseError),

    #[error("Error listing databases: {0}")]
    ListDatabaseError(#[from] ListDatabaseError),

    #[error("Error reading file {:?}: {}", file_name, source)]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[error("Error writing: {0}")]
    WriteError(#[from] WriteError),

    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error in chunk subcommand: {0}")]
    Chunk(#[from] chunk::Error),

    #[error("Error in partition subcommand: {0}")]
    Partition(#[from] partition::Error),

    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Error in partition subcommand: {0}")]
    Catalog(#[from] recover::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Create a new database
#[derive(Debug, StructOpt)]
struct Create {
    /// The name of the database
    name: String,
    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory based on the drop_order
    #[structopt(long, default_value = "52428800")] // 52428800 = 50*1024*1024
    buffer_size_soft: usize,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    #[structopt(long, default_value = "104857600")] // 104857600 = 100*1024*1024
    buffer_size_hard: usize,

    /// Allow dropping data that has not been persisted to object storage
    /// once the database size has exceeded the configured limits
    #[structopt(long = "drop-persisted-only", parse(from_flag = std::ops::Not::not))]
    drop_non_persisted: bool,

    /// Persists chunks to object storage.
    #[structopt(long = "skip-persist", parse(from_flag = std::ops::Not::not))]
    persist: bool,

    /// Do not allow writing new data to this database
    #[structopt(long)]
    immutable: bool,

    /// After how many transactions should IOx write a new checkpoint?
    #[structopt(long, default_value = "100", parse(try_from_str))]
    catalog_transactions_until_checkpoint: NonZeroU64,

    /// Once a partition hasn't received a write for this period of time,
    /// it will be compacted and, if set, persisted. Writers will generally
    /// have this amount of time to send late arriving writes or this could
    /// be their clock skew.
    #[structopt(long, default_value = "300")]
    late_arrive_window_seconds: u32,

    /// Maximum number of rows before triggering persistence
    #[structopt(long, default_value = "100000")]
    persist_row_threshold: u64,

    /// Maximum age of a write before triggering persistence
    #[structopt(long, default_value = "1800")]
    persist_age_threshold_seconds: u32,

    /// Maximum number of rows to buffer in a MUB chunk before compacting it
    #[structopt(long, default_value = "100000")]
    mub_row_threshold: u64,

    /// Use up to this amount of space in bytes for caching Parquet files. A
    /// value of zero disables Parquet file caching.
    #[structopt(long, default_value = "0")]
    parquet_cache_limit: u64,
}

/// Get list of databases
#[derive(Debug, StructOpt)]
struct List {}

/// Return configuration of specific database
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    name: String,
}

/// Write data into the specified database
#[derive(Debug, StructOpt)]
struct Write {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

/// Query the data with SQL
#[derive(Debug, StructOpt)]
struct Query {
    /// The name of the database
    name: String,

    /// The query to run, in SQL format
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[structopt(short, long, default_value = "pretty")]
    format: String,
}

/// All possible subcommands for database
#[derive(Debug, StructOpt)]
enum Command {
    Create(Create),
    List(List),
    Get(Get),
    Write(Write),
    Query(Query),
    Chunk(chunk::Config),
    Partition(partition::Config),
    Recover(recover::Config),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::Create(command) => {
            let mut client = management::Client::new(connection);
            #[allow(deprecated)]
            let rules = DatabaseRules {
                name: command.name,
                lifecycle_rules: Some(LifecycleRules {
                    buffer_size_soft: command.buffer_size_soft as _,
                    buffer_size_hard: command.buffer_size_hard as _,
                    drop_non_persisted: command.drop_non_persisted,
                    persist: command.persist,
                    immutable: command.immutable,
                    worker_backoff_millis: Default::default(),
                    max_active_compactions_cfg: Default::default(),
                    catalog_transactions_until_checkpoint: command
                        .catalog_transactions_until_checkpoint
                        .get(),
                    late_arrive_window_seconds: command.late_arrive_window_seconds,
                    persist_row_threshold: command.persist_row_threshold,
                    persist_age_threshold_seconds: command.persist_age_threshold_seconds,
                    mub_row_threshold: command.mub_row_threshold,
                    parquet_cache_limit: command.parquet_cache_limit,
                }),

                // Default to hourly partitions
                partition_template: Some(PartitionTemplate {
                    parts: vec![partition_template::Part {
                        part: Some(partition_template::part::Part::Time(
                            "%Y-%m-%d %H:00:00".into(),
                        )),
                    }],
                }),

                // Note no write buffer config
                ..Default::default()
            };

            client.create_database(rules).await?;

            println!("Ok");
        }
        Command::List(_) => {
            let mut client = management::Client::new(connection);
            let databases = client.list_databases().await?;
            println!("{}", databases.join("\n"))
        }
        Command::Get(get) => {
            let mut client = management::Client::new(connection);
            let database = client.get_database(get.name).await?;
            println!("{}", serde_json::to_string_pretty(&database)?);
        }
        Command::Write(write) => {
            let mut client = write::Client::new(connection);

            let mut file = File::open(&write.file_name).map_err(|e| Error::ReadingFile {
                file_name: write.file_name.clone(),
                source: e,
            })?;

            let mut lp_data = String::new();
            file.read_to_string(&mut lp_data)
                .map_err(|e| Error::ReadingFile {
                    file_name: write.file_name.clone(),
                    source: e,
                })?;

            let lines_written = client.write(write.name, lp_data).await?;

            println!("{} Lines OK", lines_written);
        }
        Command::Query(query) => {
            let mut client = flight::Client::new(connection);
            let Query {
                name,
                format,
                query,
            } = query;

            let format = QueryOutputFormat::from_str(&format)?;

            let mut query_results = client.perform_query(&name, query).await?;

            // It might be nice to do some sort of streaming write
            // rather than buffering the whole thing.
            let mut batches = vec![];
            while let Some(data) = query_results.next().await? {
                batches.push(data);
            }

            let formatted_result = format.format(&batches)?;

            println!("{}", formatted_result);
        }
        Command::Chunk(config) => {
            chunk::command(connection, config).await?;
        }
        Command::Partition(config) => {
            partition::command(connection, config).await?;
        }
        Command::Recover(config) => {
            recover::command(connection, config).await?;
        }
    }

    Ok(())
}
