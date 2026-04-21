"""
TZ Planet DAG - Build timezone boundaries in GeoJSON, GeoPackage, and GeoParquet.

Downloads the latest timezone-boundary-builder GeoJSON release,
converts it to GeoPackage and GeoParquet using GDAL, and uploads
all three formats to R2.

Schedule: Daily at 01:00 UTC
Produces Assets:
  - openplanetdata-tz-planet-geojson
  - openplanetdata-tz-planet-geopackage
  - openplanetdata-tz-planet-geoparquet
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

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/tz/planet"
GEOJSON_PATH = f"{WORK_DIR}/planet-tz.geojson"
GEOPACKAGE_PATH = f"{WORK_DIR}/planet-tz.gpkg"
PARQUET_PATH = f"{WORK_DIR}/planet-tz.parquet"

GEOJSON_ASSET = Asset(
    name="openplanetdata-tz-planet-geojson",
    uri=f"s3://{R2_BUCKET}/tz/planet/geojson/v1/planet-latest.tz.geojson",
)

GEOPACKAGE_ASSET = Asset(
    name="openplanetdata-tz-planet-geopackage",
    uri=f"s3://{R2_BUCKET}/tz/planet/geopackage/v1/planet-latest.tz.gpkg",
)

GEOPARQUET_ASSET = Asset(
    name="openplanetdata-tz-planet-geoparquet",
    uri=f"s3://{R2_BUCKET}/tz/planet/geoparquet/v1/planet-latest.tz.parquet",
)

UPLOAD_TAGS = ["public", "timezone"]

with DAG(
    dag_display_name="OpenPlanetData TZ Planet",
    dag_id="openplanetdata_tz_planet",
    default_args={
        "execution_timeout": timedelta(hours=1),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_tz",
        "queue": "cortex",
        "retries": 0,
    },
    description="Build planet timezone boundaries in GeoJSON, GeoPackage, and GeoParquet",
    doc_md=__doc__,
    max_active_runs=1,
    schedule="0 1 * * *",
    tags=["geojson", "geopackage", "geoparquet", "openplanetdata", "timezone", "tz"],
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

    convert_to_geopackage = DockerOperator(
        task_id="convert_geojson_to_geopackage",
        task_display_name="Convert GeoJSON to GeoPackage",
        image=GDAL_FULL_IMAGE,
        command=f"""bash -c '
            ogr2ogr -f GPKG {GEOPACKAGE_PATH} {GEOJSON_PATH} &&
            ls -lh {GEOPACKAGE_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    convert_to_geoparquet = DockerOperator(
        task_id="convert_geojson_to_geoparquet",
        task_display_name="Convert GeoJSON to GeoParquet",
        image=GDAL_FULL_IMAGE,
        command=f"""bash -c '
            ogr2ogr -f Parquet {PARQUET_PATH} {GEOJSON_PATH} \
                    -lco GEOMETRY_ENCODING=WKB &&
            ls -lh {PARQUET_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload GeoJSON to R2",
        bucket=R2_BUCKET,
        outlets=[GEOJSON_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geojson() -> list[UploadItem]:
        """Upload TZ GeoJSON to R2."""
        return [UploadItem(
            category="timezone",
            destination_filename="planet-latest.tz.geojson",
            destination_path="tz/planet/geojson",
            destination_version="v1",
            entity="planet",
            extension="geojson",
            media_type="application/geo+json",
            source=GEOJSON_PATH,
            tags=UPLOAD_TAGS + ["geojson"],
        )]

    @task.r2index_upload(
        task_display_name="Upload GeoPackage to R2",
        bucket=R2_BUCKET,
        outlets=[GEOPACKAGE_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geopackage() -> list[UploadItem]:
        """Upload TZ GeoPackage to R2."""
        return [UploadItem(
            category="timezone",
            destination_filename="planet-latest.tz.gpkg",
            destination_path="tz/planet/geopackage",
            destination_version="v1",
            entity="planet",
            extension="gpkg",
            media_type="application/geopackage+sqlite3",
            source=GEOPACKAGE_PATH,
            tags=UPLOAD_TAGS + ["geopackage"],
        )]

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
            tags=UPLOAD_TAGS + ["geoparquet"],
        )]

    @task(task_id="tz_planet_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="tz_planet_cleanup", task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    geojson_upload = upload_geojson()
    geopackage_upload = upload_geopackage()
    geoparquet_upload = upload_geoparquet()

    download_geojson >> geojson_upload
    download_geojson >> convert_to_geopackage >> geopackage_upload
    download_geojson >> convert_to_geoparquet >> geoparquet_upload

    uploads = [geojson_upload, geopackage_upload, geoparquet_upload]
    uploads >> done()
    uploads >> cleanup()
