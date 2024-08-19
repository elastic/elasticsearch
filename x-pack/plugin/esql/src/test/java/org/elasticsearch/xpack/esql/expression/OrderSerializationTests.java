/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class OrderSerializationTests extends AbstractExpressionSerializationTests<Order> {
    public static Order randomOrder() {
        return new Order(randomSource(), randomChild(), randomDirection(), randomNulls());
    }

    @Override
    protected Order createTestInstance() {
        return randomOrder();
    }

    private static Order.OrderDirection randomDirection() {
        return randomFrom(Order.OrderDirection.values());
    }

    private static Order.NullsPosition randomNulls() {
        return randomFrom(Order.NullsPosition.values());
    }

    @Override
    protected Order mutateInstance(Order instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.child();
        Order.OrderDirection direction = instance.direction();
        Order.NullsPosition nulls = instance.nullsPosition();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, AbstractExpressionSerializationTests::randomChild);
            case 1 -> direction = randomValueOtherThan(direction, OrderSerializationTests::randomDirection);
            case 2 -> nulls = randomValueOtherThan(nulls, OrderSerializationTests::randomNulls);
        }
        return new Order(source, child, direction, nulls);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
