/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.AggregateSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.TopNAggregateSerializationTests;

import java.io.IOException;
import java.util.List;

public class TopNAggregateExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TopNAggregateExec> {
    @Override
    protected TopNAggregateExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        List<? extends NamedExpression> aggregates = AggregateSerializationTests.randomAggregates();
        AggregatorMode mode = randomFrom(AggregatorMode.values());
        List<Attribute> intermediateAttributes = randomFieldAttributes(0, 5, false);
        Integer estimatedRowSize = randomEstimatedRowSize();
        List<Order> order = TopNAggregateSerializationTests.randomOrder();
        Expression limit = FieldAttributeTests.createFieldAttribute(1, true);

        return new TopNAggregateExec(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, order, limit);
    }

    @Override
    protected TopNAggregateExec mutateInstance(TopNAggregateExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<? extends Expression> groupings = instance.groupings();
        List<? extends NamedExpression> aggregates = instance.aggregates();
        List<Attribute> intermediateAttributes = instance.intermediateAttributes();
        AggregatorMode mode = instance.getMode();
        Integer estimatedRowSize = instance.estimatedRowSize();
        List<Order> order = instance.order();
        Expression limit = instance.limit();
        switch (between(0, 7)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> groupings = randomValueOtherThan(groupings, () -> randomFieldAttributes(0, 5, false));
            case 2 -> aggregates = randomValueOtherThan(aggregates, AggregateSerializationTests::randomAggregates);
            case 3 -> mode = randomValueOtherThan(mode, () -> randomFrom(AggregatorMode.values()));
            case 4 -> intermediateAttributes = randomValueOtherThan(intermediateAttributes, () -> randomFieldAttributes(0, 5, false));
            case 5 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            case 6 -> {
                order = randomValueOtherThan(order, TopNAggregateSerializationTests::randomOrder);
            }
            case 7 -> {
                limit = FieldAttributeTests.createFieldAttribute(1, true);
            }
            default -> throw new IllegalStateException();
        }
        return new TopNAggregateExec(
            instance.source(),
            child,
            groupings,
            aggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
