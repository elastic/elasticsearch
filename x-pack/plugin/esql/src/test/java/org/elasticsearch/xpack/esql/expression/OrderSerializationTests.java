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
    @Override
    protected Order createTestInstance() {
        return new Order(randomSource(), randomChild(), randomDirection(), randomNulls());
    }

    private static org.elasticsearch.xpack.esql.core.expression.Order.OrderDirection randomDirection() {
        return randomFrom(org.elasticsearch.xpack.esql.core.expression.Order.OrderDirection.values());
    }

    private static org.elasticsearch.xpack.esql.core.expression.Order.NullsPosition randomNulls() {
        return randomFrom(org.elasticsearch.xpack.esql.core.expression.Order.NullsPosition.values());
    }

    @Override
    protected Order mutateInstance(Order instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.child();
        org.elasticsearch.xpack.esql.core.expression.Order.OrderDirection direction = instance.direction();
        org.elasticsearch.xpack.esql.core.expression.Order.NullsPosition nulls = instance.nullsPosition();
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
