import pytest
from pathlib import Path
from typing import Any, Generator, List, cast
from pyiceberg.catalog.sql import SqlCatalog
from pytest_lazyfixture import lazy_fixture
from pyiceberg.schema import Schema
from pyiceberg.typedef import Identifier
from pyiceberg.table import Table
import os
from pyiceberg.catalog import Catalog
import pyarrow as pa
from pyiceberg.io.pyarrow import _dataframe_to_data_files, schema_to_pyarrow

from pyspark.sql import SparkSession
from pyiceberg.catalog.rest import RestCatalog


@pytest.fixture(scope="module")
def catalog_memory(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": "sqlite:///:memory:",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()

@pytest.fixture(scope="module")
def catalog_sqlite_without_rowcount(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.engine.dialect.supports_sane_rowcount = False
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite_fsspec(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
        PY_IO_IMPL: FSSPEC_FILE_IO,
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(name="random_table_identifier")
def fixture_random_table_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    os.makedirs(f"{warehouse}/{database_name}.db/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name

@pytest.fixture(scope="module")
def catalog_name() -> str:
    return "test_sql_catalog"

# @pytest.mark.parametrize(
#     "catalog",
#     [
#         lazy_fixture("catalog_memory"),
#         lazy_fixture("catalog_sqlite"),
#         lazy_fixture("catalog_sqlite_without_rowcount"),
#         lazy_fixture("catalog_sqlite_fsspec"),
#     ],
# )
# @pytest.mark.parametrize(
#     "table_identifier",
#     [
#         lazy_fixture("random_table_identifier"),
#         lazy_fixture("random_hierarchical_identifier"),
#         lazy_fixture("random_table_identifier_with_catalog"),
#     ],
# )

@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_sqlite")
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier")
    ],
)

# def test_planfiles




def test_append_table(catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier) -> None:
    table_identifier_nocatalog = catalog._identifier_to_tuple_without_catalog(table_identifier)
    namespace = Catalog.namespace_from(table_identifier_nocatalog)
    catalog.create_namespace(namespace)
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    spec = PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "foo_part")
    )
    table = catalog.create_table(table_identifier, table_schema_simple, partition_spec=spec)

    df = pa.Table.from_pydict(
        {
            "foo": ["a","c", 'c'],
            "bar": [1,2,3],
            "baz": [True, False, True]
        },
        schema=schema_to_pyarrow(table_schema_simple),
    )

    table.append(df)

    # new snapshot is written in APPEND mode
    # assert len(table.metadata.snapshots) == 1
    # assert table.metadata.snapshots[0].snapshot_id == table.metadata.current_snapshot_id
    # assert table.metadata.snapshots[0].parent_snapshot_id is None
    # assert table.metadata.snapshots[0].sequence_number == 1
    # assert table.metadata.snapshots[0].summary is not None
    # assert table.metadata.snapshots[0].summary.operation == Operation.APPEND
    # assert table.metadata.snapshots[0].summary["added-data-files"] == "1"
    # assert table.metadata.snapshots[0].summary["added-records"] == "1"
    # assert table.metadata.snapshots[0].summary["total-data-files"] == "1"
    # assert table.metadata.snapshots[0].summary["total-records"] == "1"
    # assert len(table.metadata.metadata_log) == 1

    # read back the data
    assert df == table.scan().to_arrow()
    assert 3 == table.scan().count()
    #
    # table.delete()
    # assert 0 == table.scan().count()


    table.append(df)
    assert 6 == table.scan().count()

    table.append(df)
    from pyiceberg.expressions import EqualTo
    assert 3 == table.scan(row_filter=EqualTo("foo","a")).count()

    # table.delete()
    # assert 0 == table.scan().count()


    table.append(df)
    from pyiceberg.expressions import EqualTo
    assert 0 == table.scan(row_filter=EqualTo("foo","b")).count()

    table.append(df)
    table.delete(delete_filter=EqualTo('foo', 'c'))

    assert 0 == table.scan(row_filter=EqualTo("foo","b")).count()
    assert 5 == table.scan().count()

    assert len(table.scan().to_arrow()) == 5

    table.delete()



def test_count(table_v2: Table):
    scan = table_v2.scan()

@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
def test_delete_partitioned_table_positional_deletes(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = "default.table_partitioned_delete"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30), (10, 40)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 30
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    # Assert that there is just a single Parquet file, that has one merge on read file
    files = list(tbl.scan().plan_files())
    assert len(files) == 1
    assert len(files[0].delete_files) == 1
    # Will rewrite a data file without the positional delete
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10, 10], "number": [20, 40]}
    assert tbl.scan().count() == 2
    assert tbl.scan().count() == 1

    tbl.delete(EqualTo("number", 40))

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "overwrite", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10], "number": [20]}
    assert tbl.scan().count() == 1
