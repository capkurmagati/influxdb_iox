use std::convert::TryInto;

use data_types::job::Operation;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{connection::Connection, management};
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Need to pass `--force`"))]
    NeedsTheForceError,

    #[snafu(display("Error wiping persisted catalog: {}", source))]
    WipeError {
        source: management::WipePersistedCatalogError,
    },

    #[snafu(display("Error skipping replay: {}", source))]
    SkipReplayError { source: management::SkipReplayError },

    #[snafu(display("Received invalid response: {}", source))]
    InvalidResponse { source: FieldViolation },

    #[snafu(display("Error rendering response as JSON: {}", source))]
    WritingJson { source: serde_json::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Recover broken databases.
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// All possible subcommands for catalog
#[derive(Debug, StructOpt)]
enum Command {
    /// Wipe persisted catalog
    Wipe(Wipe),

    /// Skip replay
    SkipReplay(SkipReplay),
}

/// Wipe persisted catalog.
#[derive(Debug, StructOpt)]
struct Wipe {
    /// Force wipe. Required option to prevent accidental erasure
    #[structopt(long)]
    force: bool,

    /// The name of the database
    db_name: String,
}

/// Skip replay
#[derive(Debug, StructOpt)]
struct SkipReplay {
    /// The name of the database
    db_name: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = management::Client::new(connection);

    match config.command {
        Command::Wipe(wipe) => {
            let Wipe { force, db_name } = wipe;

            if !force {
                return Err(Error::NeedsTheForceError);
            }

            let operation: Operation = client
                .wipe_persisted_catalog(db_name)
                .await
                .context(WipeError)?
                .try_into()
                .context(InvalidResponse)?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation).context(WritingJson)?;
        }
        Command::SkipReplay(skip_replay) => {
            let SkipReplay { db_name } = skip_replay;

            client.skip_replay(db_name).await.context(SkipReplayError)?;

            println!("Ok");
        }
    }

    Ok(())
}
