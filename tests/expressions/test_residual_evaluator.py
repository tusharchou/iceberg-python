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
    BooleanExpression
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

    predicate = EqualTo("dateint", 20180801)

    res_eval = residual_evaluator_of(
        schema=schema,
        spec=spec,
        expr=predicate,
        case_sensitive=True
    )

    residual = res_eval.residual_for(Record(dateint=20180815))

    assert residual == AlwaysFalse()

    residual = res_eval.residual_for(Record(dateint=20180801))

    assert residual == AlwaysTrue()

    assert isinstance(residual, BooleanExpression)

    predicate = Or(
        Or(
            And(LessThan("dateint", 20180815), GreaterThan("dateint", 20170801)),
            Or(EqualTo("dateint", 20180815), LessThan("hour", 12))
        ),
        And(EqualTo("dateint", 20180801), LessThan("hour", 11))
    )
    predicate = And(LessThan("dateint", 20180815), GreaterThan("dateint", 20170801))
    res_eval = residual_evaluator_of(
        schema=schema,
        spec=spec,
        expr=predicate,
        case_sensitive=True
    )

    residual = res_eval.residual_for(Record(dateint=20180801))

    assert residual == AlwaysTrue()

    # assert isinstance(residual.as_unbound, type(EqualTo))

    residual = res_eval.residual_for(Record(dateint=20180830))

    assert residual == AlwaysFalse()


    predicate = Or(
        Or(
            And(LessThan("dateint", 20180815), GreaterThan("dateint", 20170801)),
            Or(EqualTo("dateint", 20180815), LessThan("hour", 12))
        ),
        And(EqualTo("dateint", 20180801), LessThan("hour", 11))
    )
    predicate = LessThan("hour", 12)
    res_eval = residual_evaluator_of(
        schema=schema,
        spec=spec,
        expr=predicate,
        case_sensitive=True
    )

    residual = res_eval.residual_for(Record(dateint=20180801))

    assert isinstance(residual, BoundPredicate)
    # assert isinstance(type(residual.as_unbound), type(LessThan))

    assert residual.literal.value == 12


    # assert isinstance(residual.as_unbound, type(EqualTo))

    residual = res_eval.residual_for(Record(dateint=20180830))

    assert residual.term.field.name == 'hour'
    assert residual.literal.value == 12
    from pyiceberg.expressions import BoundLessThan
    assert type(residual) == BoundLessThan


    predicate = Or(
        Or(
            And(LessThan("dateint", 20180815), GreaterThan("dateint", 20170801)),
            Or(EqualTo("dateint", 20180815), LessThan("hour", 12))
        ),
        And(EqualTo("dateint", 20180801), LessThan("hour", 11))
    )
    predicate = And(EqualTo("dateint", 20180801), LessThan("hour", 11))
    res_eval = residual_evaluator_of(
        schema=schema,
        spec=spec,
        expr=predicate,
        case_sensitive=True
    )

    residual = res_eval.residual_for(Record(dateint=20180801))

    assert isinstance(residual, BoundPredicate)
    # assert isinstance(type(residual.as_unbound), type(LessThan))

    assert residual.literal.value == 11


    # assert isinstance(residual.as_unbound, type(EqualTo))

    residual = res_eval.residual_for(Record(dateint=20180830))

    assert residual.term.field.name == 'hour'
    assert residual.literal.value == 12
    from pyiceberg.expressions import BoundLessThan
    assert type(residual) == BoundLessThan

    # bound = predicate.bind(schema)
    # assert bound.as_unbound == type(predicate)


@pytest.fixture
def schema() -> Schema:
    return Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))


@pytest.fixture
def dateint_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))


# def test_others():

# def test_residual_evaluator(dateint_spec, schema):
#     """
#     givens:
#     a. Schema
#         columns:
#             1. dateint
#             2. hour
#     b. partitioning
#         dateint_part
#     c. expression
#         1. greater than 20170801
#         2. less than 20170815
#         3. equal to 20170815 less than 12
#         4. equal to 20170801 greater than 11
#     d. expected result
#         1. if partition value = 20170801
#             a. False
#             b. True
#             c. False
#             d. greater than 11
#         2. if partition value = 20170815
#             a. True
#             b. False
#             c. less than 12
#             d. False
#     """
#
#     # Case A:
#     expression = GreaterThan(term='dateint', literal=20170801)
#     partition_literal_value = 20170801
#     partition_record = Record(dateint=partition_literal_value)
#     expectation = AlwaysFalse()
#     assert isinstance(expression, BooleanExpression)
#     assert isinstance(partition_record, Record)
#     res_eval= residual_evaluator_of(spec=dateint_spec, expr=expression, case_sensitive=True, schema=schema)
#     # assert isinstance(residual_evaluator, )
#     result = residual_evaluator(partition_record)
#     assert result == expectation
#
#     partition_literal_values = [20170815, 20170801]
#     for value in partition_literal_values:
#
#         partition_record_only_value = Record(value)
#         partition_record_with_key = Record(dateint=value,hour=1)
#
#         # source_id = partition_record_with_key.term.ref().field.field_id
#         # assert source_id == 51
#         filter_expressions = [
#             (LessThan("hour", 12), LessThan("hour", 12)),
#             (EqualTo(term="dateint", literal=value), AlwaysTrue()),
#             (LessThan("dateint", value), AlwaysFalse()),
#             (And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801)),AlwaysFalse()),
#             (LessThan("hour", 12),LessThan("hour", 12)),
#             # (And(EqualTo("dateint", 20170815), LessThan("hour", 12)),LessThan("hour", 12))
#         ]
#         for pred, expectation in filter_expressions:
#             # bound = pred.bind(schema=schema)
#             # source_id = bound.term.ref().field.field_id
#             # assert isinstance(source_id, int)
#             # parts = dateint_spec.fields_by_source_id(source_id)
#             # assert len(parts) == 1
#             # part = parts[0]
#             # part.transform()
#             # assert isinstance(bound.term, BoundReference)
#             # assert bound.term.field.field_id ==
#             # assert not dateint_spec.fields_by_source_id(bound.term.field.field_id)
#             # parts = self.spec.fields_by_source_id(pred.term.field.field_id)
#
#             # part = self.spec.get_field_by_source_id(pred.ref.field_id)
#
#             # assert isinstance(pred, UnboundPredicate)
#             # assert isinstance(bound, BoundPredicate)
#             # assert bound == pred
#
#             # (List < PartitionField >
#             # expr.bind()
#             # parts = dateint_spec.fields_by_source_id(field_id=dateint_spec.source_id_to_fields_map)
#             # .getFieldsBySourceId(pred.ref().fieldId())
#             # residual_evaluator = residual_eval(spec=dateint_spec, expr=bound, case_sensitive=True, schema=schema)
#             res_eval = residual_evaluator(spec=dateint_spec, expr=pred, case_sensitive=True, schema=schema)
#             result = residual_evaluator(partition_record_with_key)
#
#             assert isinstance(result, BooleanExpression)
#
#             # assert isinstance(result, BoundPredicate)
#             # assert result == expectation.bind(schema)
#             # assert result.as_unbound() == expectation
#
#             result = residual_evaluator(partition_record_only_value)
#             # assert result == expectation.bind(schema)
