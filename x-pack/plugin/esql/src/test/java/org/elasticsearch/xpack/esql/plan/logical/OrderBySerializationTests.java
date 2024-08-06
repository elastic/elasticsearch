/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;

import java.io.IOException;
import java.util.List;

public class OrderBySerializationTests extends AbstractLogicalPlanSerializationTests<OrderBy> {
    @Override
    protected OrderBy createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Order> orders = randomList(1, 10, OrderSerializationTests::randomOrder);
        return new OrderBy(source, child, orders);
    }

    @Override
    protected OrderBy mutateInstance(OrderBy instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Order> orders = instance.order();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            orders = randomValueOtherThan(orders, () -> randomList(1, 10, OrderSerializationTests::randomOrder));
        }
        return new OrderBy(instance.source(), child, orders);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
