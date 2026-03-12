/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;

import java.io.IOException;
import java.util.List;

public class TopNExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TopNExec> {
    public static TopNExec randomTopNExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Order> order = randomList(1, 10, OrderSerializationTests::randomOrder);
        Expression limit = new Literal(randomSource(), randomNonNegativeInt(), DataType.INTEGER);
        Integer estimatedRowSize = randomEstimatedRowSize();
        List<Expression> groupings = randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList();
        return new TopNExec(source, child, order, limit, groupings, estimatedRowSize);
    }

    @Override
    protected TopNExec createTestInstance() {
        return randomTopNExec(0);
    }

    @Override
    protected TopNExec mutateInstance(TopNExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Order> order = instance.order();
        Expression limit = instance.limit();
        Integer estimatedRowSize = instance.estimatedRowSize();
        List<Expression> groupings = instance.groupings();
        InputOrdering inputOrdering = instance.inputOrdering();

        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> order = randomValueOtherThan(order, () -> randomList(1, 10, OrderSerializationTests::randomOrder));
            case 2 -> limit = randomValueOtherThan(limit, () -> new Literal(randomSource(), randomNonNegativeInt(), DataType.INTEGER));
            case 3 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            case 4 -> groupings = randomValueOtherThan(
                groupings,
                () -> randomFieldAttributes(0, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 5 -> inputOrdering = (inputOrdering == InputOrdering.SORTED ? InputOrdering.NOT_SORTED : InputOrdering.SORTED);
            default -> throw new UnsupportedOperationException();
        }
        var result = new TopNExec(instance.source(), child, order, limit, groupings, estimatedRowSize);
        return inputOrdering == InputOrdering.SORTED ? result.withSortedInput() : result.withNonSortedInput();
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
