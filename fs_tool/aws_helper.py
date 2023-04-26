from typing import List, Dict, Any
import boto3
from sagemaker.session import Session
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.feature_store.feature_definition import (
    FeatureDefinition,
    StringFeatureDefinition,
    FractionalFeatureDefinition,
)

from fs_tool.dsl.compilation import Parser

REGION_NAME = Session().boto_region_name
BUCKET_NAME = Session().default_bucket()
PREFIX = "feature-store"
boto_session = boto3.Session(region_name=REGION_NAME)


def get_role_arn() -> str:
    """Retrieve the SageMaker role."""

    role_arn = None
    roles = boto_session.client("iam").list_roles()["Roles"]
    for role in roles:
        if "AmazonSageMaker-ExecutionRole" in role["RoleName"]:
            role_arn = role["Arn"]
            break
    if role_arn is None:
        raise RuntimeError("Error: Could not find AmazonSageMaker-ExecutionRole role.")
    else:
        return role_arn


def get_feature_definitions(parser: Parser) -> List[FeatureDefinition]:
    """Create feature definitions from Feature objects."""

    feature_definitions = []
    for feature in parser.features:
        feature_definitions.append(FractionalFeatureDefinition(feature.name))

    return feature_definitions


def create_feature_group(parser: Parser) -> Dict[str, Any]:
    """Create a Feature Group from the parsed config file."""

    sagemaker_client = boto_session.client(service_name="sagemaker", region_name=REGION_NAME)
    feature_store_runtime = boto_session.client(service_name="sagemaker-featurestore-runtime", region_name=REGION_NAME)
    feature_store_session = Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        sagemaker_featurestore_runtime_client=feature_store_runtime,
    )

    feature_definitions = [
        StringFeatureDefinition("_".join(parser.fg_entity_columns)),
        StringFeatureDefinition(parser.fg_time_column),
    ]
    feature_definitions.extend(get_feature_definitions(parser))
    feature_group = FeatureGroup(
        name=parser.fg_name,
        sagemaker_session=feature_store_session,
        feature_definitions=feature_definitions,
    )

    feature_group.create(
        description=parser.fg_description,
        s3_uri=f"s3://{BUCKET_NAME}/{PREFIX}",
        record_identifier_name="_".join(parser.fg_entity_columns),
        event_time_feature_name=parser.fg_time_column,
        enable_online_store=True,
        role_arn=get_role_arn(),
    )

    return feature_group.describe()


def list_feature_groups() -> Dict[str, Any]:
    """List all the existing Feature Groups."""

    client = boto_session.client("sagemaker")
    try:
        return client.list_feature_groups()
    except Exception as e:
        raise RuntimeError("Error: Could not list the feature groups.") from e


def delete_feature_group(fg_name: str) -> Dict[str, Any]:
    """Delete a Feature Group given its name."""

    client = boto_session.client("sagemaker")
    try:
        return client.delete_feature_group(FeatureGroupName=fg_name)
    except Exception as e:
        raise RuntimeError(f"Error: Could not delete the feature group {fg_name}.") from e


def describe_feature_group(fg_name: str) -> Dict[str, Any]:
    """Describe a Feature Group given its name."""

    client = boto_session.client("sagemaker")
    try:
        return client.describe_feature_group(FeatureGroupName=fg_name)
    except Exception as e:
        raise RuntimeError(f"Error: Could not find the feature group {fg_name}.") from e
