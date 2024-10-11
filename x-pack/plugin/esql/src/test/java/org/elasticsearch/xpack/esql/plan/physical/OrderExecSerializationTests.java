/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;

import java.io.IOException;
import java.util.List;

public class OrderExecSerializationTests extends AbstractPhysicalPlanSerializationTests<OrderExec> {
    public static OrderExec randomOrderExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Order> order = randomList(1, 10, OrderSerializationTests::randomOrder);
        return new OrderExec(source, child, order);
    }

    @Override
    protected OrderExec createTestInstance() {
        return randomOrderExec(0);
    }

    @Override
    protected OrderExec mutateInstance(OrderExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Order> order = instance.order();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            order = randomValueOtherThan(order, () -> randomList(1, 10, OrderSerializationTests::randomOrder));
        }
        return new OrderExec(instance.source(), child, order);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
