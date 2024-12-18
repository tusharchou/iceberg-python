from abc import ABC
from pyiceberg.expressions.visitors import (
    BoundBooleanExpressionVisitor,
    BooleanExpression,
    UnboundPredicate,
    BoundPredicate,
    visit,
    BoundTerm,
    AlwaysFalse,
    AlwaysTrue
)
from pyiceberg.expressions.literals import Literal
from pyiceberg.expressions import (
    And,
    Or
)
from pyiceberg.types import L
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from typing import Any, List, Set
from pyiceberg.typedef import Record


class ResidualVisitor(BoundBooleanExpressionVisitor[BooleanExpression], ABC):
    schema: Schema
    spec: PartitionSpec
    case_sensitive: bool

    def __init__(self, schema: Schema, spec: PartitionSpec, case_sensitive: bool, expr: BooleanExpression):
        self.schema = schema
        self.spec = spec
        self.case_sensitive = case_sensitive
        self.expr = expr

    # def project(self, expr: BooleanExpression) -> BooleanExpression:
    #     #  projections assume that there are no NOT nodes in the expression tree. to ensure that this
    #     #  is the case, the expression is rewritten to push all NOT nodes down to the expression
    #     #  leaf nodes.
    #     #  this is necessary to ensure that the default expression returned when a predicate can't be
    #     #  projected is correct.
    #     return visit(bind(self.schema, rewrite_not(expr), self.case_sensitive), self)

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
        bound = predicate.bind(self.schema, case_sensitive=True)

        if isinstance(bound, BoundPredicate):
            bound_residual = visit(bound,self)
            bound_residual = self.visit_bound_predicate(predicate=bound)
            # if isinstance(bound_residual, BooleanExpression):
            #     return predicate

            return bound_residual


        # return bound

    # def visitor(self):
    #     if not hasattr(self.thread_local_data, "visitors"):
    #         self.thread_local_data.visitors = ResidualVisitor()
    #
    #     return self.thread_local_data.visitors

    def visit_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) == literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_not_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        return term.eval(self.struct) != literal.value


    def visit_less_than(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) < literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_greater_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) >= literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_greater_than(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) > literal.value:
            return self.visit_true()
        else:
            return self.visit_false()

    def visit_less_than(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) < literal.value:
            return self.visit_true()
        else:
            return self.visit_false()
    def visit_less_than_or_equal(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        if term.eval(self.struct) <= literal.value:
            return self.visit_true()
        else:
            return self.visit_false()
    def visit_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        eval_res = term.eval(self.struct)
        return eval_res is not None and str(eval_res).startswith(str(literal.value))

    def visit_not_starts_with(self, term: BoundTerm[L], literal: Literal[L]) -> bool:
        return not self.visit_starts_with(term, literal)


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


    def eval(self, partition_data: Record):
        self.struct = partition_data
        return visit(self.expr, visitor=self)

    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> List[str]:
        """
        called from eval
        input
        """
        parts = self.spec.fields_by_source_id(predicate.term.ref().field.field_id)
        if parts == []:
            return predicate

        for part in parts:
            strict_projection = part.transform.strict_project(part.name, predicate)
            strict_result = None
            # return strict_projection
            if strict_projection is not None:
                from pyiceberg.types import StructType
                def struct_to_schema(struct: StructType) -> Schema:
                    return Schema(*[f for f in struct.fields])

                bound = strict_projection.bind(struct_to_schema(self.spec.partition_type(self.schema)))
                from pyiceberg.expressions import AlwaysFalse
                # return AlwaysFalse()
                # return bound
                assert isinstance(bound, BoundPredicate)
                if isinstance(bound, BoundPredicate):

                    strict_result = super().visit_bound_predicate(bound)
                    return strict_result
                else:
                    strict_result = bound


            if strict_result is not None and isinstance(strict_result, AlwaysTrue):
                return AlwaysTrue()
            inclusive_projection = part.transform.project(part.name, predicate)
            inclusive_result = None
            if inclusive_projection is not None:
                bound_inclusive = inclusive_projection.bind(struct_to_schema(self.spec.partition_type(self.schema)))
                if isinstance(bound_inclusive, BoundPredicate):
                    inclusive_result = super().visit_bound_predicate(bound_inclusive)
                    return inclusive_result
                else:
                    inclusive_result = bound_inclusive
            if inclusive_result is not None and isinstance(inclusive_result, AlwaysFalse):
                return AlwaysFalse()

        # return predicate





class ResidualEvaluator(ResidualVisitor):
    def residual_for(self, partition_data):
        return self.eval(partition_data)

def residual_evaluator_of(
        spec: PartitionSpec,
        expr: BooleanExpression,
        case_sensitive: bool,
        schema: Schema
) -> ResidualEvaluator:
    return ResidualEvaluator(spec=spec, expr=expr, schema=schema, case_sensitive=case_sensitive)

