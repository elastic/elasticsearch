/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;

public class CombineLimitTopNTests extends AbstractLogicalPlanOptimizerTests {

    public void testCombineLimitByTopNBySameGroupings() {
        var attr = getFieldAttribute("a");
        var groupings = List.<Expression>of(attr);
        var order = List.of(new Order(EMPTY, attr, Order.OrderDirection.ASC, null));
        var topNBy = new TopNBy(EMPTY, emptySource(), order, L(5), groupings);
        var limitBy = new LimitBy(EMPTY, L(10), topNBy, groupings);
        var result = new CombineLimitTopN().rule(limitBy);
        var resultTopNBy = as(result, TopNBy.class);
        assertThat(resultTopNBy.limitPerGroup(), equalTo(L(5)));
        assertThat(resultTopNBy.groupings(), equalTo(groupings));
    }

    public void testCombineLimitByTopNByDifferentGroupings() {
        var attr = getFieldAttribute("a");
        var groupings = List.<Expression>of(attr);
        var otherGroupings = List.<Expression>of(getFieldAttribute("b"));
        var order = List.of(new Order(EMPTY, attr, Order.OrderDirection.ASC, null));
        var topNBy = new TopNBy(EMPTY, emptySource(), order, L(5), groupings);
        var limitBy = new LimitBy(EMPTY, L(10), topNBy, otherGroupings);
        var result = new CombineLimitTopN().rule(limitBy);
        var resultLimitBy = as(result, LimitBy.class);
        assertThat(resultLimitBy.limitPerGroup(), equalTo(L(10)));
        var resultTopNBy = as(resultLimitBy.child(), TopNBy.class);
        assertThat(resultTopNBy.groupings(), equalTo(groupings));
    }

    /**
     * When Limit and TopN have semantically equal groupings (same logical attribute, different expression instances, different order),
     * the rule should still combine them. Ensures Expressions.semanticEquals(List, List) is used.
     */
    public void testCombineLimitTopNSemanticallyEqualGroupings() {
        var a = getFieldAttribute("a");
        var b = getFieldAttribute("b");
        var topNGroupings = List.<Expression>of(b, a);
        var limitGroupings = List.<Expression>of(
            new ReferenceAttribute(EMPTY, null, a.name(), a.dataType(), a.nullable(), a.id(), false),
            new ReferenceAttribute(EMPTY, null, b.name(), b.dataType(), b.nullable(), b.id(), false)
        );
        var source = emptySource();
        var topNBy = new TopNBy(EMPTY, source, List.of(new Order(EMPTY, a, Order.OrderDirection.ASC, null)), L(5), topNGroupings);
        var limitBy = new LimitBy(EMPTY, L(10), topNBy, limitGroupings);
        var result = new CombineLimitTopN().rule(limitBy);
        var resultTopNBy = as(result, TopNBy.class);
        assertThat(resultTopNBy.limitPerGroup(), equalTo(L(5)));
        assertThat(resultTopNBy.child(), equalTo(source));
        assertThat(resultTopNBy.groupings(), equalTo(topNGroupings));
    }

    public void testLowerLimitIsChosenForCombiningTopNByAndLimitBy() {
        var attr = getFieldAttribute("a");
        var groupings = List.<Expression>of(attr);
        var order = List.of(new Order(EMPTY, attr, Order.OrderDirection.ASC, null));
        var topNBy = new TopNBy(EMPTY, emptySource(), order, L(10), groupings);
        var limitBy = new LimitBy(EMPTY, L(5), topNBy, groupings);
        var result = new CombineLimitTopN().rule(limitBy);
        var resultTopNBy = as(result, TopNBy.class);
        assertThat(resultTopNBy.limitPerGroup(), equalTo(L(5)));
        assertThat(resultTopNBy.groupings(), equalTo(groupings));
    }

    public void testLowerLimitIsChosenForCombiningTopNByAndLimitBy2() {
        var attr = getFieldAttribute("a");
        var groupings = List.<Expression>of(attr);
        var order = List.of(new Order(EMPTY, attr, Order.OrderDirection.ASC, null));
        var topNBy = new TopNBy(EMPTY, emptySource(), order, L(5), groupings);
        var limitBy = new LimitBy(EMPTY, L(10), topNBy, groupings);
        var result = new CombineLimitTopN().rule(limitBy);
        var resultTopNBy = as(result, TopNBy.class);
        assertThat(resultTopNBy.limitPerGroup(), equalTo(L(5)));
        assertThat(resultTopNBy.groupings(), equalTo(groupings));
    }
}
