use anyhow::Context;
use aws_crt_s3::common::allocator::Allocator;
use aws_crt_s3::sdkutils::endpoints_rule_engine::{RequestContext, RuleEngine, Ruleset, Value};
use aws_crt_s3::sdkutils::partitions::PartitionsConfig;

use clap::{Arg, Command};

fn main() -> anyhow::Result<()> {
    let matches = Command::new("endpoint_resolver")
        .about("Resolve an endpoint from the given rules files")
        .arg(
            Arg::new("partitions")
                .required(true)
                .help("Path to the partitions.json file"),
        )
        .arg(Arg::new("rules").required(true).help("Path to the ruleset JSON file"))
        .arg(Arg::new("parameters").takes_value(true).multiple_values(true))
        .get_matches();

    let partitions_path = matches.get_one::<String>("partitions").unwrap();
    let partitions = std::fs::read_to_string(partitions_path)?;
    let partitions_config =
        PartitionsConfig::new(&Allocator::default(), &partitions).context("failed to parse partitions config")?;

    let ruleset_path = matches.get_one::<String>("rules").unwrap();
    let ruleset = std::fs::read_to_string(ruleset_path)?;
    let ruleset = Ruleset::new(&Allocator::default(), &ruleset).context("failed to parse ruleset")?;

    let engine =
        RuleEngine::new(&Allocator::default(), &ruleset, &partitions_config).context("failed to build rule engine")?;

    let mut context = RequestContext::new(&Allocator::default()).context("failed to create request context")?;

    for param in matches.values_of("parameters").into_iter().flatten() {
        let (key, value) = param.split_once('=').context("parameters should be 'key=value'")?;
        let value = match value {
            "true" => Value::Boolean(true),
            "false" => Value::Boolean(false),
            s => Value::String(s),
        };
        context
            .insert(&Allocator::default(), key, value)
            .context("failed to add parameter {param}")?;
    }

    let resolved = engine.resolve(&context)?;

    println!("resolved URL: {:?}", resolved.url().unwrap());

    Ok(())
}
