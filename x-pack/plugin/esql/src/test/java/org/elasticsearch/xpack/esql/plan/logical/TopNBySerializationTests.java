/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;

import java.io.IOException;
import java.util.List;

public class TopNBySerializationTests extends AbstractLogicalPlanSerializationTests<TopNBy> {
    public static TopNBy randomTopNBy(int depth) {
        Source source = randomSource();
        LogicalPlan child = randomChild(depth);
        List<Order> order = randomOrders();
        Expression limit = AbstractExpressionSerializationTests.randomChild();
        List<Expression> groupings = randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList();
        return new TopNBy(source, child, order, limit, groupings);
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
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> order = randomValueOtherThan(order, TopNBySerializationTests::randomOrders);
            case 2 -> limit = randomValueOtherThan(limit, AbstractExpressionSerializationTests::randomChild);
            case 3 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList()
            );
        }
        return new TopNBy(source, child, order, limit, groupings);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

}
