# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from abc import ABC
from typing import Any, Set

from pyiceberg.expressions import And, Or
from pyiceberg.expressions.literals import Literal
from pyiceberg.expressions.visitors import (
    AlwaysFalse,
    AlwaysTrue,
    BooleanExpression,
    BoundBooleanExpressionVisitor,
    BoundPredicate,
    BoundTerm,
    Not,
    UnboundPredicate,
    visit,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import L


class ResidualVisitor(BoundBooleanExpressionVisitor[BooleanExpression], ABC):
    schema: Schema
    spec: PartitionSpec
    case_sensitive: bool

    def __init__(self, schema: Schema, spec: PartitionSpec, case_sensitive: bool, expr: BooleanExpression):
        self.schema = schema
        self.spec = spec
        self.case_sensitive = case_sensitive
        self.expr = expr

    def eval(self, partition_data: Record) -> BooleanExpression:
        self.struct = partition_data
        return visit(self.expr, visitor=self)

    def visit_true(self) -> BooleanExpression:
        return AlwaysTrue()

    def visit_false(self) -> BooleanExpression:
        return AlwaysFalse()

    def visit_not(self, child_result: BooleanExpression) -> BooleanExpression:
        return Not(child_result)

    def visit_and(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return And(left_result, right_result)

    def visit_or(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return Or(left_result, right_result)

    def visit_is_null(self, term: BoundTerm[L]) -> BooleanExpression:
        if term.eval(self.struct) is None:
            return AlwaysTrue()
        else:
            return AlwaysFalse()

    def visit_not_null(self, term: BoundTerm[L]) -> BooleanExpression:
        if term.eval(self.struct) is not None:
            return AlwaysTrue()
        else:
            return AlwaysFalse()

    def visit_is_nan(self, term: BoundTerm[L]) -> BooleanExpression:
        val = term.eval(self.struct)
        if val is None:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_not_nan(self, term: BoundTerm[L]) -> BooleanExpression:
        val = term.eval(self.struct)
        if val is not None:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_less_than(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) < literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_less_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) <= literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_greater_than(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) > literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_greater_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) >= literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_equal(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) == literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_not_equal(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if term.eval(self.struct) != literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_in(self, term: BoundTerm[L], literals: Set[L]) -> BooleanExpression:
        if term.eval(self.struct) in literals:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_not_in(self, term: BoundTerm[L], literals: Set[L]) -> BooleanExpression:
        if term.eval(self.struct) not in literals:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        eval_res = term.eval(self.struct)
        if eval_res is not None and str(eval_res).startswith(str(literal.value)):
            return AlwaysTrue()
        else:
            return AlwaysFalse()

    def visit_not_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> BooleanExpression:
        if not self.visit_starts_with(term, literal):
            return AlwaysTrue()
        else:
            return AlwaysFalse()

    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> BooleanExpression:
        """
        If there is no strict projection or if it evaluates to false, then return the predicate.

        Get the strict projection and inclusive projection of this predicate in partition data,
        then use them to determine whether to return the original predicate. The strict projection
        returns true iff the original predicate would have returned true, so the predicate can be
        eliminated if the strict projection evaluates to true. Similarly the inclusive projection
        returns false iff the original predicate would have returned false, so the predicate can
        also be eliminated if the inclusive projection evaluates to false.

        """
        parts = self.spec.fields_by_source_id(predicate.term.ref().field.field_id)
        if parts == []:
            return predicate

        from pyiceberg.types import StructType

        def struct_to_schema(struct: StructType) -> Schema:
            return Schema(*[f for f in struct.fields])

        for part in parts:
            strict_projection = part.transform.strict_project(part.name, predicate)
            strict_result = None

            if strict_projection is not None:
                bound = strict_projection.bind(struct_to_schema(self.spec.partition_type(self.schema)))
                if isinstance(bound, BoundPredicate):
                    strict_result = super().visit_bound_predicate(bound)
                else:
                    strict_result = bound

            if strict_result is not None and isinstance(strict_result, AlwaysTrue):
                return AlwaysTrue()

            inclusive_projection = part.transform.project(part.name, predicate)
            inclusive_result = None
            if inclusive_projection is not None:
                bound_inclusive = inclusive_projection.bind(struct_to_schema(self.spec.partition_type(self.schema)))
                if isinstance(bound_inclusive, BoundPredicate):
                    # using predicate method specific to inclusive
                    inclusive_result = super().visit_bound_predicate(bound_inclusive)
                else:
                    # if the result is not a predicate, then it must be a constant like alwaysTrue or
                    # alwaysFalse
                    inclusive_result = bound_inclusive
            if inclusive_result is not None and isinstance(inclusive_result, AlwaysFalse):
                return AlwaysFalse()

        return predicate

    def visit_unbound_predicate(self, predicate: UnboundPredicate[L]) -> BooleanExpression:
        bound = predicate.bind(self.schema, case_sensitive=True)

        if isinstance(bound, BoundPredicate):
            bound_residual = self.visit_bound_predicate(predicate=bound)
            # if isinstance(bound_residual, BooleanExpression):
            if bound_residual not in (AlwaysFalse(), AlwaysTrue()):
                # replace inclusive original unbound predicate
                return predicate

            # use the non-predicate residual (e.g. alwaysTrue)
            return bound_residual

        # if binding didn't result in a Predicate, return the expression
        return bound


class ResidualEvaluator(ResidualVisitor):
    def residual_for(self, partition_data: Record) -> BooleanExpression:
        return self.eval(partition_data)


class UnpartitionedResidualEvaluator(ResidualEvaluator):
    # Finds the residuals for an Expression the partitions in the given PartitionSpec
    def __init__(self, schema: Schema, expr: BooleanExpression):
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC

        super().__init__(schema=schema, spec=UNPARTITIONED_PARTITION_SPEC, expr=expr, case_sensitive=False)
        self.expr = expr

    def residual_for(self, partition_data: Record) -> BooleanExpression:
        return self.expr


def residual_evaluator_of(
    spec: PartitionSpec, expr: BooleanExpression, case_sensitive: bool, schema: Schema
) -> ResidualEvaluator:
    if len(spec.fields) != 0:
        return ResidualEvaluator(spec=spec, expr=expr, schema=schema, case_sensitive=case_sensitive)
    else:
        return UnpartitionedResidualEvaluator(schema=schema, expr=expr)
