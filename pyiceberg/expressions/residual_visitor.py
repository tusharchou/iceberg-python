from .visitors import (
    BoundBooleanExpressionVisitor,
    BooleanExpressionVisitor,
    BooleanExpression,
    BoundTerm,
    visit,
    ProjectionEvaluator,
    bind
)
from abc import ABC
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.types import (
    StructType
)
from pyiceberg.expressions.literals import Literal
from pyiceberg.schema import Schema
from pyiceberg.expressions import (
    UnboundPredicate,
    BoundPredicate
)
from typing import Set, Any
from pyiceberg.typedef import L
from pyiceberg.struct_like import StructLike
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundLiteralPredicate,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundNotStartsWith,
    BoundPredicate,
    BoundSetPredicate,
    BoundStartsWith,
    BoundTerm,
    BoundUnaryPredicate,
    Not,
    Or,
    UnboundPredicate,
)
from typing import List

class ResidualVisitor(BoundBooleanExpressionVisitor):

    def __init__(self, spec:PartitionSpec = None, expr: BooleanExpression = None ,data_struct: StructLike = None):
        self.visit_history: List[str] = []

        self.struct = data_struct
        self.expr = expr
        self.spec = spec

    def eval(self, data_struct, schema) -> BooleanExpression:
        self.struct = data_struct
        bound =  bind(schema=schema, expression=self.expr, case_sensitive=True)

        return visit(bound, self)

    def visit_in(self, term: BoundTerm[L], literals: Set[L]) -> bool:
        return term.eval(self.struct) in literals

    def visit_not_in(self, term: BoundTerm[L], literals: Set[L]) -> bool:
        return term.eval(self.struct) not in literals

    def visit_is_nan(self, term: BoundTerm[L]) -> bool:
        val = term.eval(self.struct)
        return val != val

    def visit_not_nan(self, term: BoundTerm[L]) -> bool:
        val = term.eval(self.struct)
        return val == val

    def visit_is_null(self, term: BoundTerm[L]) -> bool:
        return term.eval(self.struct) is None

    def visit_not_null(self, term: BoundTerm[L]) -> bool:
        return term.eval(self.struct) is not None

    def visit_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        self.visit_history.append('EQUAL')
        result =  self.always_true() if ref.get(self.struct) == lit.value else self.always_false()
        return result , self.visit_history
        # if term.eval(self.struct) == literal.value:
        #     return self.visit_true()
        # else:
        #     return self.visit_false()

    def visit_not_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        return term.eval(self.struct) != literal.value

    def visit_greater_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        value = term.eval(self.struct)
        return value is not None and value >= literal.value

    def visit_greater_than(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        value = term.eval(self.struct)
        return value is not None and value > literal.value

    def visit_less_than(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        value = term.eval(self.struct)
        return value is not None and value < literal.value

    def visit_less_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        value = term.eval(self.struct)
        return value is not None and value <= literal.value

    def visit_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        eval_res = term.eval(self.struct)
        return eval_res is not None and str(eval_res).startswith(str(literal.value))

    def visit_not_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        return not self.visit_starts_with(term, literal)

    def visit_not(self, child_result: bool) -> bool:
        return not child_result


    def visit_true(self) -> BooleanExpression:
        return AlwaysTrue()

    def visit_false(self) -> BooleanExpression:
        return AlwaysFalse()

    # def visit_not(self, child_result: BooleanExpression) -> BooleanExpression:
    #     raise ValueError(f"Cannot project not expression, should be rewritten: {child_result}")
    #
    def visit_and(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        self.visit_history.append("AND")
        # return self.visit_history
        return And(left_result, right_result), self.visit_history

    def visit_or(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return Or(left_result, right_result)

    # def visit_unbound_predicate(self, predicate: UnboundPredicate[L]) -> BooleanExpression:
    #
    #     raise ValueError(f"Cannot project unbound predicate: {predicate}")


    #
    # def visit_unbound_predicate(self, predicate: UnboundPredicate[L]) -> BooleanExpression:
    #     return  predicate
    # #     return predicate.bind(self.schema, case_sensitive=self.case_sensitive)

    def predicate(self, pred) -> BooleanExpression:
        """
        Get the strict projection and inclusive projection of this predicate in partition data,
        then use them to determine whether to return the original predicate. The strict projection
        returns true iff the original predicate would have returned true, so the predicate can be
        eliminated if the strict projection evaluates to true. Similarly the inclusive projection
        returns false iff the original predicate would have returned false, so the predicate can
        also be eliminated if the inclusive projection evaluates to false.
        If there is no strict projection or if it evaluates to false, then return the predicate.
        """

        if isinstance(pred, BoundPredicate):
            return self.bound_predicate(pred)
        elif isinstance(pred, UnboundPredicate):
            return self.unbound_predicate(pred)

        raise RuntimeError("Invalid predicate argument %s" % pred)

        def bound_predicate(self, pred):
            part = self.spec.get_field_by_source_id(pred.ref.field_id)
            if part is None:
                return pred

            strict_projection = part.transform.project_strict(part.name, pred)
            if strict_projection is None:
                bound = strict_projection.bind(self.spec.partition_type())
                if isinstance(bound, BoundPredicate):
                    return super(ResidualVisitor, self).predicate(bound)
                return bound

            return pred

        def unbound_predicate(self, pred):
            bound = pred.bind(self.spec.schema.as_struct())

            if isinstance(bound, BoundPredicate):
                bound_residual = self.predicate(bound)
                if isinstance(bound_residual, Predicate):
                    return pred
                return bound_residual

            return bound


    def residuals(self, expr: BooleanExpression) -> BooleanExpression:
        """
        expression is a tree
        there shouldn't be any NOT nodes in the expression tree.
        push all NOT nodes down to the expression leaf node
        this is necessary to ensure that the default expression returned when a predicate can't be
        projected is correct
        """
        return visit(bind(self.schema, rewrite_not(expr), self.case_sensitive), self)

    def visit_unbound_predicate(self, predicate: UnboundPredicate[Any]) -> List[str]:
        self.visit_history.append(str(predicate.__class__.__name__).upper()+' unbounded_predicate')
        # bound = predicate.bind(self.schema, case_sensitive=self.case_sensitive)
        # if isinstance(bound, BoundPredicate):
        # bound = predicate.bind(self.spec.schema.as_struct())

        if isinstance(bound, BoundPredicate):
            bound_residual = self.predicate(bound)
            if isinstance(bound_residual, Predicate):
                return pred, self.visit_history
            return bound_residual, self.visit_history

        return bound, self.visit_history


        # return predicate, self.visit_history

    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> List[str]:
        self.visit_history.append(str(predicate.__class__.__name__).upper()+' bound_predicate')
        return predicate, self.visit_history




class ResidualEvaluator(BooleanExpressionVisitor[BooleanExpression], ABC):
    """
    Finds the residuals for an Expression the partitions in the given PartitionSpec.
    A residual expression is made by partially evaluating an expression using partition values.

    For example, if a table is partitioned by day(utc_timestamp) and is read with a filter expression
    utc_timestamp &gt;= a and utc_timestamp &lt;= b, then there are 4 possible residuals expressions
    for the partition data, d:
     * <ul>
     *   <li>If d &gt; day(a) and d &lt; day(b), the residual is always true
     *   <li>If d == day(a) and d != day(b), the residual is utc_timestamp &gt;= a
     *   <li>if d == day(b) and d != day(a), the residual is utc_timestamp &lt;= b
     *   <li>If d == day(a) == day(b), the residual is utc_timestamp &gt;= a and utc_timestamp &lt;= b
     * </ul>
     *
    Partition data is passed using StructLike. Residuals are returned by residualFor(StructLike).
    """

    def __init__(self, spec: PartitionSpec, expr: BooleanExpression, case_sensitive: bool, schema: Schema):
        self.spec = spec
        self.expr = expr
        self.case_sensitive = case_sensitive
        self.schema = schema
        # self.visitor = self.visitor()

    def visitor(self, data_struct: StructType):
        return ResidualVisitor(data_struct)

    from pyiceberg.struct_like import StructLike

    def residual_for(self, partition_data: StructLike) -> BooleanExpression:
        """
        usage
        input: Row object
        output: Operation over expression
        """
        visitor = ResidualVisitor(expr = self.expr, spec = self.spec)
        return visitor.eval(data_struct=partition_data, schema=self.schema)

    def visit_true(self) -> BooleanExpression:
        return AlwaysTrue()

    def visit_false(self) -> BooleanExpression:
        return AlwaysFalse()

    def visit_not(self, child_result: BooleanExpression) -> BooleanExpression:
        raise ValueError(f"Cannot project not expression, should be rewritten: {child_result}")

    def visit_and(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return And(left_result, right_result)

    def visit_or(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return Or(left_result, right_result)

    def visit_unbound_predicate(self, predicate: UnboundPredicate[L]) -> BooleanExpression:
        raise ValueError(f"Cannot project unbound predicate: {predicate}")

    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> BooleanExpression:
        parts = self.spec.fields_by_source_id(predicate.term.ref().field.field_id)

        result: BooleanExpression = AlwaysFalse()
        for part in parts:
            # consider (ts > 2019-01-01T01:00:00) with day(ts) and hour(ts)
            # projections: d >= 2019-01-02 and h >= 2019-01-01-02 (note the inclusive bounds).
            # any timestamp where either projection predicate is true must match the original
            # predicate. For example, ts = 2019-01-01T03:00:00 matches the hour projection but not
            # the day, but does match the original predicate.
            strict_projection = part.transform.strict_project(name=part.name, pred=predicate)
            if strict_projection is not None:
                result = Or(result, strict_projection)

        return result

    def as_unbound(self) -> UnboundPredicate:
        return UnboundPredicate(self.expr)


class UnpartitionedResidualEvaluator(ResidualEvaluator):

    def __init__(self, expr: BooleanExpression):
        super().__init__(UNPARTITIONED_PARTITION_SPEC, expr, False)
        self.expr = expr

    def residual_for(self) -> BooleanExpression:
        return self.expr


def unpartitioned(expr: BooleanExpression) -> ResidualEvaluator:
    return UnpartitionedResidualEvaluator(expr)


    # def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> BooleanExpression:
    #     parts = self.spec.fields_by_source_id(predicate.term.ref().field.field_id)
    #
    #     result: BooleanExpression = AlwaysTrue()
    #     for part in parts:
    #         # consider (d = 2019-01-01) with bucket(7, d) and bucket(5, d)
    #         # projections: b1 = bucket(7, '2019-01-01') = 5, b2 = bucket(5, '2019-01-01') = 0
    #         # any value where b1 != 5 or any value where b2 != 0 cannot be the '2019-01-01'
    #         #
    #         # similarly, if partitioning by day(ts) and hour(ts), the more restrictive
    #         # projection should be used. ts = 2019-01-01T01:00:00 produces day=2019-01-01 and
    #         # hour=2019-01-01-01. the value will be in 2019-01-01-01 and not in 2019-01-01-02.
    #         incl_projection = part.transform.project(name=part.name, pred=predicate)
    #         if incl_projection is not None:
    #             result = And(result, incl_projection)
    #
    #     return result


def residual_of(
    spec: PartitionSpec, expr: BooleanExpression, case_sensitive: bool, schema: Schema
) -> ResidualEvaluator:
    if spec.fields:
        return ResidualEvaluator(spec=spec, expr=expr, case_sensitive=case_sensitive, schema=schema)
    else:
        return UnpartitionedResidualEvaluator(expr)


class StrictResidual(ResidualEvaluator):
    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> BooleanExpression:
        parts = self.spec.fields_by_source_id(predicate.term.ref().field.field_id)

        result: BooleanExpression = AlwaysFalse()
        for part in parts:
            # consider (ts > 2019-01-01T01:00:00) with day(ts) and hour(ts)
            # projections: d >= 2019-01-02 and h >= 2019-01-01-02 (note the inclusive bounds).
            # any timestamp where either projection predicate is true must match the original
            # predicate. For example, ts = 2019-01-01T03:00:00 matches the hour projection but not
            # the day, but does match the original predicate.
            strict_residual = part.transform.strict_residual(name=part.name, pred=predicate)
            if strict_residual is not None:
                result = Or(result, strict_residual)

        return result
