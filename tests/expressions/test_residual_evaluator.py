import pytest

from pyiceberg.expressions import AlwaysTrue, EqualTo, LessThan, AlwaysFalse
from pyiceberg.expressions.residual_visitor import residual_eval
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField


@pytest.fixture
def schema() -> Schema:
    return Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))


@pytest.fixture
def dateint_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))


def test_residual_evaluator(dateint_spec, schema):
    partition_literal_values = [20170815, 20170801]
    for value in partition_literal_values:
        partition_record_only_value = Record(value)
        partition_record_with_key = Record(dateint=value)

        filter_expressions = [
            (EqualTo(term="dateint", literal=value), AlwaysTrue()),
            (LessThan("dateint", value), AlwaysFalse())
        ]
        for expr, expectation in filter_expressions:
            residual_evaluator = residual_eval(spec=dateint_spec, expr=expr, case_sensitive=True, schema=schema)
            result = residual_evaluator(partition_record_with_key)
            assert result == expectation

            result = residual_evaluator(partition_record_only_value)
            assert result == expectation
