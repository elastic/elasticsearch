/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;
import org.elasticsearch.xpack.esql.expression.Partition;
import org.elasticsearch.xpack.esql.expression.PartitionSerializationTests;

import java.io.IOException;
import java.util.List;

public class TopNExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TopNExec> {
    public static TopNExec randomTopNExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Partition> partition = randomList(1, 10, PartitionSerializationTests::randomPartition);
        List<Order> order = randomList(1, 10, OrderSerializationTests::randomOrder);
        Expression limit = new Literal(randomSource(), randomNonNegativeInt(), DataType.INTEGER);
        Integer estimatedRowSize = randomEstimatedRowSize();
        return new TopNExec(source, child, partition, order, limit, estimatedRowSize);
    }

    @Override
    protected TopNExec createTestInstance() {
        return randomTopNExec(0);
    }

    @Override
    protected TopNExec mutateInstance(TopNExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Partition> partition = instance.partition();
        List<Order> order = instance.order();
        Expression limit = instance.limit();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> partition = randomValueOtherThan(partition, () -> randomList(1, 10, PartitionSerializationTests::randomPartition));
            case 2 -> order = randomValueOtherThan(order, () -> randomList(1, 10, OrderSerializationTests::randomOrder));
            case 3 -> limit = randomValueOtherThan(limit, () -> new Literal(randomSource(), randomNonNegativeInt(), DataType.INTEGER));
            case 4 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            default -> throw new UnsupportedOperationException();
        }
        return new TopNExec(instance.source(), child, partition, order, limit, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
