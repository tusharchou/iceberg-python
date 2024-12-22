#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint:disable=redefined-outer-name

import pytest

from pyiceberg.expressions import (
    AlwaysTrue,
    AlwaysFalse,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNull,
    Or,
)
from pyiceberg.expressions.visitors import residual_of, ResidualEvaluator, UnboundPredicate, visit, BindVisitor
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    TruncateTransform,
)
from pyiceberg.types import (
    IntegerType,
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


# @pytest.fixture
# def schema() -> Schema:
#     return Schema(
#         NestedField(1, "id", LongType(), required=False),
#         NestedField(2, "data", StringType(), required=False),
#         NestedField(3, "event_date", DateType(), required=False),
#         NestedField(4, "event_ts", TimestampType(), required=False),
#     )


@pytest.fixture
def schema() -> Schema:
    return Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )


@pytest.fixture
def empty_spec() -> PartitionSpec:
    return PartitionSpec()

@pytest.fixture
def dateint_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))

# @pytest.fixture
# def id_spec() -> PartitionSpec:
#     return PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "id_part"))


@pytest.fixture
def bucket_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(2, 1000, BucketTransform(16), "data_bucket"))


@pytest.fixture
def day_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(4, 1000, DayTransform(), "date"), PartitionField(3, 1000, DayTransform(), "ddate"))


@pytest.fixture
def hour_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(4, 1000, HourTransform(), "hour"))


@pytest.fixture
def truncate_str_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(2, 1000, TruncateTransform(2), "data_trunc"))


@pytest.fixture
def truncate_int_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(1, 1000, TruncateTransform(10), "id_trunc"))


@pytest.fixture
def id_and_bucket_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "id_part"), PartitionField(2, 1001, BucketTransform(16), "data_bucket")
    )


# def test_identity_residual(schema: Schema, id_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("id"),
#         IsNull("id"),
#         LessThan("id", 100),
#         LessThanOrEqual("id", 101),
#         GreaterThan("id", 102),
#         GreaterThanOrEqual("id", 103),
#         EqualTo("id", 104),
#         NotEqualTo("id", 105),
#         In("id", {3, 4, 5}),
#         NotIn("id", {3, 4, 5}),
#     ]
#
#     expected = [
#         NotNull("id_part"),
#         IsNull("id_part"),
#         LessThan("id_part", 100),
#         LessThanOrEqual("id_part", 101),
#         GreaterThan("id_part", 102),
#         GreaterThanOrEqual("id_part", 103),
#         EqualTo("id_part", 104),
#         NotEqualTo("id_part", 105),
#         In("id_part", {3, 4, 5}),
#         NotIn("id_part", {3, 4, 5}),
#     ]
#
#     residual = inclusive_residual(schema, id_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr


def test_identity_transform_residual(row_of, schema: Schema, dateint_spec: PartitionSpec) -> None:
    # residual =
    predicates = [
        NotNull("dateint"),
        IsNull("dateint"),
        LessThan("dateint", 100),
        LessThanOrEqual("dateint", 101),
        GreaterThan("dateint", 102),
        GreaterThanOrEqual("dateint", 103),
        EqualTo("dateint", 104),
        NotEqualTo("dateint", 105),
        In("dateint", {3, 4, 5}),
        NotIn("dateint", {3, 4, 5}),
    ]

    expected = [
        NotNull("dateint_part"),
        IsNull("dateint_part"),
        LessThan("dateint_part", 100),
        LessThanOrEqual("dateint_part", 101),
        GreaterThan("dateint_part", 102),
        GreaterThanOrEqual("dateint_part", 103),
        EqualTo("dateint_part", 104),
        NotEqualTo("dateint_part", 105),
        In("dateint_part", {3, 4, 5}),
        NotIn("dateint_part", {3, 4, 5}),
    ]

    # residual = inclusive_residual(schema, dateint_spec)
    # for index, predicate in enumerate(predicates):
    #     expr = residual(predicate)
    #     assert expected[index] == expr

    custom_expr = Or(
        Or(
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801)),
            And(EqualTo("dateint", 20170815), LessThan("hour", 12))),
            And(EqualTo("dateint", 20170801), GreaterThan("hour", 11))
    )
    res_eval = residual_of(
        dateint_spec,
        custom_expr
    )
    residual_expression = res_eval.residual_for(row_of(20170815), schema=schema)
    # unbound = res_eval.as_unbound()

    assert residual_expression == UnboundPredicate
    assert type(residual_expression) == Or



    unbound_expression = Or(AlwaysTrue(), AlwaysFalse())
    assert type(unbound_expression) == AlwaysTrue
    bound_expression = visit(unbound_expression, visitor=BindVisitor(schema=schema, case_sensitive=True))
    assert bound_expression == AlwaysTrue()
    bound_expression = visit(custom_expr, visitor=BindVisitor(schema=schema, case_sensitive=True))
    # unbound = expr
    # assert type(unbound) == type(UnboundPredicate)


    # from pyiceberg.expressions import _to_unbound_term
    # term = _to_unbound_term(expr)
    # unbound = UnboundPredicate(term)
    # from pyiceberg.expressions.visitors import bind
    # bound = bind(schema=schema, expression=expr, case_sensitive=False)

    # unbound = residual.as_unbound()

    # equal to the upper date bound
    # Expression residual = resEval.residualFor(Row.of(20170815));
    # UnboundPredicate<?> unbound = assertAndUnwrapUnbound(residual);

    # assert (unbound.op()).as("Residual should be hour < 12")LT)
    # assertThat(unbound.ref().name()).as("Residual should be hour < 12").isEqualTo("hour");
    # assertThat(unbound.literal().value()).as("Residual should be hour < 12").isEqualTo(12);



# def test_bucket_residual(schema: Schema, bucket_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("data"),
#         IsNull("data"),
#         LessThan("data", "val"),
#         LessThanOrEqual("data", "val"),
#         GreaterThan("data", "val"),
#         GreaterThanOrEqual("data", "val"),
#         EqualTo("data", "val"),
#         NotEqualTo("data", "val"),
#         In("data", {"v1", "v2", "v3"}),
#         NotIn("data", {"v1", "v2", "v3"}),
#     ]
#
#     expected = [
#         NotNull("data_bucket"),
#         IsNull("data_bucket"),
#         AlwaysTrue(),
#         AlwaysTrue(),
#         AlwaysTrue(),
#         AlwaysTrue(),
#         EqualTo("data_bucket", 14),
#         AlwaysTrue(),
#         In("data_bucket", {1, 3, 13}),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, bucket_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr
#
#
# def test_hour_residual(schema: Schema, hour_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("event_ts"),
#         IsNull("event_ts"),
#         LessThan("event_ts", "2022-11-27T10:00:00"),
#         LessThanOrEqual("event_ts", "2022-11-27T10:00:00"),
#         GreaterThan("event_ts", "2022-11-27T09:59:59.999999"),
#         GreaterThanOrEqual("event_ts", "2022-11-27T09:59:59.999999"),
#         EqualTo("event_ts", "2022-11-27T10:00:00"),
#         NotEqualTo("event_ts", "2022-11-27T10:00:00"),
#         In("event_ts", {"2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"}),
#         NotIn("event_ts", {"2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"}),
#     ]
#
#     expected = [
#         NotNull("hour"),
#         IsNull("hour"),
#         LessThanOrEqual("hour", 463761),
#         LessThanOrEqual("hour", 463762),
#         GreaterThanOrEqual("hour", 463762),
#         GreaterThanOrEqual("hour", 463761),
#         EqualTo("hour", 463762),
#         AlwaysTrue(),
#         In("hour", {463761, 463762}),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, hour_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr, predicate
#
#
# def test_day_residual(schema: Schema, day_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("event_ts"),
#         IsNull("event_ts"),
#         LessThan("event_ts", "2022-11-27T00:00:00"),
#         LessThanOrEqual("event_ts", "2022-11-27T00:00:00"),
#         GreaterThan("event_ts", "2022-11-26T23:59:59.999999"),
#         GreaterThanOrEqual("event_ts", "2022-11-26T23:59:59.999999"),
#         EqualTo("event_ts", "2022-11-27T10:00:00"),
#         NotEqualTo("event_ts", "2022-11-27T10:00:00"),
#         In("event_ts", {"2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"}),
#         NotIn("event_ts", {"2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"}),
#     ]
#
#     expected = [
#         NotNull("date"),
#         IsNull("date"),
#         LessThanOrEqual("date", 19322),
#         LessThanOrEqual("date", 19323),
#         GreaterThanOrEqual("date", 19323),
#         GreaterThanOrEqual("date", 19322),
#         EqualTo("date", 19323),
#         AlwaysTrue(),
#         In("date", {19322, 19323}),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, day_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr, predicate
#
#
# def test_date_day_residual(schema: Schema, day_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("event_date"),
#         IsNull("event_date"),
#         LessThan("event_date", "2022-11-27"),
#         LessThanOrEqual("event_date", "2022-11-27"),
#         GreaterThan("event_date", "2022-11-26"),
#         GreaterThanOrEqual("event_date", "2022-11-26"),
#         EqualTo("event_date", "2022-11-27"),
#         NotEqualTo("event_date", "2022-11-27"),
#         In("event_date", {"2022-11-26", "2022-11-27"}),
#         NotIn("event_date", {"2022-11-26", "2022-11-27"}),
#     ]
#
#     expected = [
#         NotNull("ddate"),
#         IsNull("ddate"),
#         LessThanOrEqual("ddate", 19322),
#         LessThanOrEqual("ddate", 19323),
#         GreaterThanOrEqual("ddate", 19323),
#         GreaterThanOrEqual("ddate", 19322),
#         EqualTo("ddate", 19323),
#         AlwaysTrue(),
#         In("ddate", {19322, 19323}),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, day_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr, predicate
#
#
# def test_string_truncate_residual(schema: Schema, truncate_str_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("data"),
#         IsNull("data"),
#         LessThan("data", "aaa"),
#         LessThanOrEqual("data", "aaa"),
#         GreaterThan("data", "aaa"),
#         GreaterThanOrEqual("data", "aaa"),
#         EqualTo("data", "aaa"),
#         NotEqualTo("data", "aaa"),
#         In("data", {"aaa", "aab"}),
#         NotIn("data", {"aaa", "aab"}),
#     ]
#
#     expected = [
#         NotNull("data_trunc"),
#         IsNull("data_trunc"),
#         LessThanOrEqual("data_trunc", "aa"),
#         LessThanOrEqual("data_trunc", "aa"),
#         GreaterThanOrEqual("data_trunc", "aa"),
#         GreaterThanOrEqual("data_trunc", "aa"),
#         EqualTo("data_trunc", "aa"),
#         AlwaysTrue(),
#         EqualTo("data_trunc", "aa"),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, truncate_str_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr, predicate
#
#
# def test_int_truncate_residual(schema: Schema, truncate_int_spec: PartitionSpec) -> None:
#     predicates = [
#         NotNull("id"),
#         IsNull("id"),
#         LessThan("id", 10),
#         LessThanOrEqual("id", 10),
#         GreaterThan("id", 9),
#         GreaterThanOrEqual("id", 10),
#         EqualTo("id", 15),
#         NotEqualTo("id", 15),
#         In("id", {15, 16}),
#         NotIn("id", {15, 16}),
#     ]
#
#     expected = [
#         NotNull("id_trunc"),
#         IsNull("id_trunc"),
#         LessThanOrEqual("id_trunc", 0),
#         LessThanOrEqual("id_trunc", 10),
#         GreaterThanOrEqual("id_trunc", 10),
#         GreaterThanOrEqual("id_trunc", 10),
#         EqualTo("id_trunc", 10),
#         AlwaysTrue(),
#         EqualTo("id_trunc", 10),
#         AlwaysTrue(),
#     ]
#
#     residual = inclusive_residual(schema, truncate_int_spec)
#     for index, predicate in enumerate(predicates):
#         expr = residual(predicate)
#         assert expected[index] == expr, predicate
#
#
# def test_residual_case_sensitive(schema: Schema, id_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_spec)
#     with pytest.raises(ValueError) as exc_info:
#         residual(NotNull("ID"))
#         assert str(exc_info) == "Could not find field with name ID, case_sensitive=True"
#
#
# def test_residual_case_insensitive(schema: Schema, id_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_spec, case_sensitive=False)
#     assert NotNull("id_part") == residual(NotNull("ID"))
#
#
# def test_residual_empty_spec(schema: Schema, empty_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, empty_spec)
#     assert AlwaysTrue() == residual(And(LessThan("id", 5), NotNull("data")))
#
#
# def test_and_residual_multiple_projected_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_and_bucket_spec)
#     assert residual(And(LessThan("id", 5), In("data", {"a", "b", "c"}))) == And(
#         LessThan("id_part", 5), In("data_bucket", {2, 3, 15})
#     )
#
#
# def test_or_residual_multiple_residual_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_and_bucket_spec)
#     assert residual(Or(LessThan("id", 5), In("data", {"a", "b", "c"}))) == Or(
#         LessThan("id_part", 5), In("data_bucket", {2, 3, 15})
#     )
#
#
# def test_not_residual_multiple_projected_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_and_bucket_spec)
#     # Not causes In to be rewritten to NotIn, which cannot be projected
#     assert residual(Not(Or(LessThan("id", 5), In("data", {"a", "b", "c"})))) == GreaterThanOrEqual("id_part", 5)


# def test_residual_partial_projected_fields(schema: Schema, id_spec: PartitionSpec) -> None:
#     residual = inclusive_residual(schema, id_spec)
#     assert residual(And(LessThan("id", 5), In("data", {"a", "b", "c"}))) == LessThan("id_part", 5)
