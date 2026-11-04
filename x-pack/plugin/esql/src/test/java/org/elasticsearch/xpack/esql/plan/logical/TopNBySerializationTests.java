/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TopNBySerializationTests extends AbstractLogicalPlanSerializationTests<TopNBy> {
    public static TopNBy randomTopNBy(int depth) {
        Source source = randomSource();
        LogicalPlan child = randomChild(depth);
        List<Order> order = randomOrders();
        Expression limit = AbstractExpressionSerializationTests.randomChild();
        List<Expression> groupings = randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList();
        return new TopNBy(source, child, order, limit, groupings, randomBoolean());
    }

    private static List<Order> randomOrders() {
        return randomList(1, 10, OrderSerializationTests::randomOrder);
    }

    @Override
    protected TopNBy createTestInstance() {
        return randomTopNBy(0);
    }

    @Override
    protected TopNBy mutateInstance(TopNBy instance) throws IOException {
        Source source = instance.source();
        LogicalPlan child = instance.child();
        List<Order> order = instance.order();
        Expression limit = instance.limitPerGroup();
        List<Expression> groupings = instance.groupings();
        boolean withTies = instance.withTies();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> order = randomValueOtherThan(order, TopNBySerializationTests::randomOrders);
            case 2 -> limit = randomValueOtherThan(limit, AbstractExpressionSerializationTests::randomChild);
            case 3 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 4 -> withTies = withTies == false;
        }
        return new TopNBy(source, child, order, limit, groupings, withTies);
    }

    public void testBackcompatWithoutTies() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(TopNBy.ESQL_TOPN_BY_WITH_TIES);
        TopNBy original = withTies(randomTopNBy(0), false);
        assertThat(copyInstance(original, oldVersion), equalTo(original));
    }

    public void testBackcompatRejectsTies() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(TopNBy.ESQL_TOPN_BY_WITH_TIES);
        TopNBy original = withTies(randomTopNBy(0), true);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(original, oldVersion));
        assertThat(e.getMessage(), containsString("SORT | LIMIT BY with ties is not supported in peer node's version [" + oldVersion));
    }

    private static TopNBy withTies(TopNBy topNBy, boolean withTies) {
        return new TopNBy(topNBy.source(), topNBy.child(), topNBy.order(), topNBy.limitPerGroup(), topNBy.groupings(), withTies);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

}
