/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Placement of the coordinator-pinned inference commands relative to a sort and a limit.
 * <p>
 * The rules involved key on {@link InferencePlan}, not on the concrete command, so COMPLETION, RERANK and DENSE_VECTOR are all
 * treated alike. These tests pin that: each assertion runs the same plan skeleton through both DENSE_VECTOR and COMPLETION and
 * requires the two shapes to agree, so a change that moves one without the other fails here.
 * <p>
 * What the shape has to achieve is that inference runs over the limited rows rather than the whole input, since every row costs
 * a call to an external endpoint. That is delivered by {@link PushDownAndCombineLimits} pushing the LIMIT <em>below</em> the
 * inference plan rather than by any rule lifting the command above the sort.
 */
public class PushDownInferencePlanTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * {@code ... | SORT y | <inference> | LIMIT 10}: the limit must end up beneath the command, so only 10 rows are sent for
     * inference. Were the command pushed under the sort first, it would embed every row and then discard all but 10.
     */
    public void testLimitIsPushedBelowInferencePlanOverASort() {
        for (UnaryOperator<LogicalPlan> command : commands()) {
            LogicalPlan sorted = command.apply(new OrderBy(EMPTY, relation(), List.of()));
            LogicalPlan plan = new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), sorted);

            LogicalPlan optimized = new PushDownAndCombineLimits().apply(plan, unboundLogicalOptimizerContext());

            InferencePlan<?> inference = as(optimized, InferencePlan.class);
            assertThat(inference.child(), instanceOf(Limit.class));
        }
    }

    /**
     * The same skeleton must optimize to the same shape whichever inference command is used; the rules are typed on the base
     * class, so a divergence here means one command has grown special-casing the others have not.
     */
    public void testDenseVectorAndCompletionShareTheSameShape() {
        EsRelation relation = relation();

        LogicalPlan denseVectorPlan = new PushDownAndCombineLimits().apply(
            new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), denseVector(new OrderBy(EMPTY, relation, List.of()))),
            unboundLogicalOptimizerContext()
        );
        LogicalPlan completionPlan = new PushDownAndCombineLimits().apply(
            new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), completion(new OrderBy(EMPTY, relation, List.of()))),
            unboundLogicalOptimizerContext()
        );

        assertEquals(shapeOf(completionPlan), shapeOf(denseVectorPlan));
    }

    /**
     * With no limit to push, the command is free to move below the sort: nothing bounds the row count either way, so the
     * placement carries no inference cost.
     */
    public void testInferencePlanMovesBelowASortWhenThereIsNoLimit() {
        for (UnaryOperator<LogicalPlan> command : commands()) {
            LogicalPlan optimized = new PushDownInferencePlan().apply(command.apply(new OrderBy(EMPTY, relation(), List.of())));

            OrderBy orderBy = as(optimized, OrderBy.class);
            assertThat(orderBy.child(), instanceOf(InferencePlan.class));
        }
    }

    private static List<UnaryOperator<LogicalPlan>> commands() {
        return List.of(PushDownInferencePlanTests::denseVector, PushDownInferencePlanTests::completion);
    }

    /**
     * Node class names from root to leaf, with every inference command collapsed to a single name so that plans differing only
     * in which command they use compare equal.
     */
    private static List<String> shapeOf(LogicalPlan plan) {
        List<String> shape = new ArrayList<>();
        LogicalPlan current = plan;
        while (current != null) {
            shape.add(current instanceof InferencePlan<?> ? "InferencePlan" : current.getClass().getSimpleName());
            current = current.children().isEmpty() ? null : current.children().get(0);
        }
        return shape;
    }

    private static DenseVector denseVector(LogicalPlan child) {
        List<NamedExpression> fields = List.of(referenceAttribute("field", DataType.KEYWORD));
        return new DenseVector(EMPTY, child, new Literal(EMPTY, 1000, DataType.INTEGER), fields);
    }

    private static Completion completion(LogicalPlan child) {
        return new Completion(
            EMPTY,
            child,
            Literal.keyword(EMPTY, "inference-id"),
            new Literal(EMPTY, 1000, DataType.INTEGER),
            Literal.keyword(EMPTY, "prompt"),
            referenceAttribute("completion", DataType.KEYWORD)
        );
    }

    private static EsRelation relation() {
        return new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.<Attribute>of(getFieldAttribute("field", DataType.KEYWORD))
        );
    }
}
