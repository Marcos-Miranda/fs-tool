import click
from fs_tool.dsl.compilation import Parser
from fs_tool.aws_helper import (
    create_feature_group,
    delete_feature_group,
    list_feature_groups,
    describe_feature_group,
    calculate_features,
)
import traceback
import os
import pickle
import sys
from pprint import pformat


@click.group()
@click.option(
    "-e",
    "--exec-id",
    "exec_id",
    default="temp",
    show_default=True,
    help="An identifier for the execution's session. The compilation state is persisted for the exec-id.",
)
@click.pass_context
def cli(ctx, exec_id):
    """Tool for interacting with SageMaker Feature Store and calculating historical features."""

    ctx.ensure_object(dict)
    ctx.obj["id"] = exec_id
    ctx.obj["path"] = os.path.expanduser(f"~/.fstool/{exec_id}")
    os.makedirs(ctx.obj["path"], exist_ok=True)


@cli.command()
@click.argument("configfile", type=click.Path(exists=True))
@click.pass_context
def compile(ctx, configfile):
    """Parse the CONFIGFILE in order to create the Feature Group and calculate the features defined on it."""

    try:
        parser = Parser(click.format_filename(configfile))
        parser.parse()
        pickle.dump(parser, open(f"{ctx.obj['path']}/parser.pkl", "wb"))
        click.secho("Compilation was successful.", fg="green")
    except Exception as e:
        click.echo(traceback.format_exc())
        click.secho(e, fg="red")


@cli.command()
@click.pass_context
def show_features(ctx):
    """Show the parsed features from the CONFIGFILE."""

    try:
        parser = pickle.load(open(f"{ctx.obj['path']}/parser.pkl", "rb"))
        for feature in parser.features:
            click.secho(feature, fg="yellow")
    except Exception:
        click.secho(f"Error: No compilation for the exec-id '{ctx.obj['id']}' has been done yet.", fg="red")


@cli.command()
@click.pass_context
def create_feat_group(ctx):
    """Create a Feature Group from the parsed CONFIGFILE."""

    try:
        parser = pickle.load(open(f"{ctx.obj['path']}/parser.pkl", "rb"))
    except Exception:
        click.secho(f"Error: No compilation for the exec-id '{ctx.obj['id']}' has been done yet.", fg="red")
        sys.exit()

    try:
        description = create_feature_group(parser)
        click.secho("Feature group created successfully.", fg="green")
        click.echo(pformat(description))
    except Exception:
        click.echo(traceback.format_exc())
        click.secho("Error: The feature group could not be created.", fg="red")


@cli.command()
@click.pass_context
def calculate_ingest_feats(ctx):
    """Calculate the features defined in the CONFIGFILE and ingest them into the Feature Store."""

    try:
        parser = pickle.load(open(f"{ctx.obj['path']}/parser.pkl", "rb"))
    except Exception:
        click.secho(f"Error: No compilation for the exec-id '{ctx.obj['id']}' has been done yet.", fg="red")
        sys.exit()

    try:
        _ = describe_feature_group(parser.fg_name)
    except Exception:
        click.secho("Error: The Feature Group has not been created yet.", fg="red")
        sys.exit()

    try:
        calculate_features(parser)
    except Exception:
        click.echo(traceback.format_exc())
        click.secho("Error: Could not calculate the features.", fg="red")


@cli.command()
@click.argument("fg_name", type=str)
def describe_feat_group(fg_name):
    """Describe the Feature Group FG_NAME."""

    try:
        description = describe_feature_group(fg_name)
        click.echo(pformat(description))
    except Exception as e:
        click.echo(traceback.format_exc())
        click.secho(e, fg="red")


@cli.command()
@click.argument("fg_name", type=str)
def delete_feat_group(fg_name):
    """Delete the Feature Group FG_NAME."""

    try:
        delete_feature_group(fg_name)
        click.secho("Feature group deleted successfully.", fg="green")
    except Exception as e:
        click.echo(traceback.format_exc())
        click.secho(e, fg="red")


@cli.command()
def list_feat_groups():
    """List all the existing Feature Groups."""

    try:
        click.echo(pformat(list_feature_groups()))
    except Exception as e:
        click.echo(traceback.format_exc())
        click.secho(e, fg="red")


if __name__ == "__main__":
    cli()
