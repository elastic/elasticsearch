/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ReplaceOrderByExpressionWithEvalTests extends AbstractLogicalPlanOptimizerTests {

    public ReplaceOrderByExpressionWithEvalTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * A {@link TopNBy} built with an expression sort key, as PromQL {@code topk} over a filter comparison builds it, sorts by an
     * {@link Eval} alias of that expression and keeps its limit, groupings and output.
     */
    public void testTopNByExpressionMovedToEval() {
        var value = getFieldAttribute("value");
        var step = getFieldAttribute("step");
        var source = emptySource();
        var key = new ToDouble(EMPTY, value);
        var topNBy = new TopNBy(EMPTY, source, List.of(new Order(EMPTY, key, Order.OrderDirection.DESC, null)), L(3), List.of(step));

        var project = as(new ReplaceOrderByExpressionWithEval().rule(topNBy), Project.class);
        assertThat(project.output(), equalTo(topNBy.output()));
        var rewritten = as(project.child(), TopNBy.class);
        assertThat(rewritten.limitPerGroup(), equalTo(L(3)));
        assertThat(rewritten.groupings(), equalTo(List.<Expression>of(step)));
        var eval = as(rewritten.child(), Eval.class);
        assertThat(eval.child(), sameInstance(source));
        var alias = eval.fields().getFirst();
        assertThat(alias.child(), equalTo(key));
        Order order = rewritten.order().getFirst();
        assertThat(order.child(), equalTo(alias.toAttribute()));
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
    }

    public void testTopNByAttributeUnchanged() {
        var value = getFieldAttribute("value");
        var order = List.of(new Order(EMPTY, value, Order.OrderDirection.ASC, null));
        var topNBy = new TopNBy(EMPTY, emptySource(), order, L(3), List.of(getFieldAttribute("step")));

        assertThat(new ReplaceOrderByExpressionWithEval().rule(topNBy), sameInstance(topNBy));
    }
}
