"""
TZ GeoParquet DAG - Build timezone boundaries GeoParquet.

Downloads the latest timezone-boundary-builder GeoJSON release,
converts it to GeoParquet using GDAL, and uploads the result to R2.

Schedule: Daily at 01:00 UTC
Produces Asset: openplanetdata-tz-planet-geoparquet
"""

import shutil
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, task
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import UploadItem
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    GDAL_FULL_IMAGE,
    OPENPLANETDATA_IMAGE,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/tz/geoparquet"
PARQUET_PATH = f"{WORK_DIR}/planet-tz.parquet"

GEOPARQUET_ASSET = Asset(
    name="openplanetdata-tz-planet-geoparquet",
    uri=f"s3://{R2_BUCKET}/tz/planet/geoparquet/v1/planet-latest.tz.parquet",
)

with DAG(
    dag_display_name="OpenPlanetData TZ GeoParquet",
    dag_id="openplanetdata_tz_geoparquet",
    default_args={
        "execution_timeout": timedelta(hours=1),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_tz",
        "queue": "cortex",
        "retries": 0,
    },
    description="Build timezone boundaries GeoParquet from timezone-boundary-builder",
    doc_md=__doc__,
    max_active_runs=1,
    schedule="0 1 * * *",
    tags=["geoparquet", "openplanetdata", "timezone", "tz"],
) as dag:

    download_geojson = DockerOperator(
        task_id="download_tz_geojson",
        task_display_name="Download Timezone GeoJSON",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            mkdir -p {WORK_DIR} &&
            cd {WORK_DIR} &&
            curl -L -o tz.zip \
                https://github.com/evansiroky/timezone-boundary-builder/releases/latest/download/timezones-with-oceans.geojson.zip &&
            unzip -j -o tz.zip -d . &&
            mv combined-with-oceans.json planet-tz.geojson &&
            rm -f tz.zip &&
            ls -lh planet-tz.geojson
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    convert_to_parquet = DockerOperator(
        task_id="convert_geojson_to_geoparquet",
        task_display_name="Convert GeoJSON to GeoParquet",
        image=GDAL_FULL_IMAGE,
        command=f"""bash -c '
            ogr2ogr -f Parquet {PARQUET_PATH} {WORK_DIR}/planet-tz.geojson \
                    -lco GEOMETRY_ENCODING=WKB &&
            ls -lh {PARQUET_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload GeoParquet to R2",
        bucket=R2_BUCKET,
        outlets=[GEOPARQUET_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geoparquet() -> list[UploadItem]:
        """Upload TZ GeoParquet to R2."""
        return [UploadItem(
            category="timezone",
            destination_filename="planet-latest.tz.parquet",
            destination_path="tz/planet/geoparquet",
            destination_version="v1",
            entity="planet",
            extension="parquet",
            media_type="application/vnd.apache.parquet",
            source=PARQUET_PATH,
            tags=["geoparquet", "public", "timezone"],
        )]

    @task(task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_geojson >> convert_to_parquet
    upload_result = upload_geoparquet()
    convert_to_parquet >> upload_result
    upload_result >> done()
    upload_result >> cleanup()
