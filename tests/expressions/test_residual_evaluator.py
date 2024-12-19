import pytest
from pyiceberg.expressions import (
    AlwaysTrue,
    EqualTo,
    LessThan,
    AlwaysFalse,
    And,
    Or,
    GreaterThan,
    UnboundPredicate,
    BoundPredicate,
    BoundReference,
    BooleanExpression,
    BoundLessThan,
    BoundGreaterThan
)
from pyiceberg.expressions.residual_evaluator import residual_evaluator_of
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField


def test_identity_transform_residual():

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "dateint_part")
    )

    predicate = Or(
        Or(
            And(EqualTo("dateint", 20170815), LessThan("hour", 12)),
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801))
        ),
        And(EqualTo("dateint", 20170801), GreaterThan("hour", 11))
    )
    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(dateint=20170815))

    assert residual.term.field.name == 'hour'
    assert residual.literal.value == 12
    assert type(residual) == BoundLessThan

    residual = res_eval.residual_for(Record(dateint=20170801))

    assert residual.term.field.name == 'hour'
    assert residual.literal.value == 11
    assert type(residual) == BoundGreaterThan

    residual = res_eval.residual_for(Record(dateint=20170812))

    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(dateint=20170817))

    assert residual == AlwaysFalse()





