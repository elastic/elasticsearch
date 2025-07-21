/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopNAggregateSerializationTests extends AbstractLogicalPlanSerializationTests<TopNAggregate> {
    @Override
    protected TopNAggregate createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = AggregateSerializationTests.randomAggregates();
        List<Order> order = randomOrder();
        Expression limit = FieldAttributeTests.createFieldAttribute(1, true);

        return new TopNAggregate(source, child, groupings, aggregates, order, limit);
    }

    public static List<Order> randomOrder() {
        int size = between(1, 5);
        List<Order> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Expression field = FieldAttributeTests.createFieldAttribute(1, true);
            Order.OrderDirection direction = randomFrom(Order.OrderDirection.values());
            Order.NullsPosition nullsPosition = randomFrom(Order.NullsPosition.values());
            result.add(new Order(randomSource(), field, direction, nullsPosition));
        }
        return result;
    }

    @Override
    protected TopNAggregate mutateInstance(TopNAggregate instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        List<Order> order = instance.order();
        Expression limit = instance.limit();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> order = randomValueOtherThan(order, TopNAggregateSerializationTests::randomOrder);
            case 4 -> limit = randomValueOtherThan(limit, () -> FieldAttributeTests.createFieldAttribute(1, true));
        }
        return new TopNAggregate(instance.source(), child, groupings, aggregates, order, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
