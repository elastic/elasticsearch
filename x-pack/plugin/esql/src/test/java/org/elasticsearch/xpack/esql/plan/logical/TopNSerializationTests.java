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
import org.elasticsearch.xpack.esql.expression.Partition;
import org.elasticsearch.xpack.esql.expression.PartitionSerializationTests;

import java.io.IOException;
import java.util.List;

public class TopNSerializationTests extends AbstractLogicalPlanSerializationTests<TopN> {
    public static TopN randomTopN(int depth) {
        Source source = randomSource();
        LogicalPlan child = randomChild(depth);
        List<Partition> partition = randomPartitions();
        List<Order> order = randomOrders();
        Expression limit = AbstractExpressionSerializationTests.randomChild();
        return new TopN(source, child, partition, order, limit);
    }

    private static List<Partition> randomPartitions() {
        return randomList(1, 10, PartitionSerializationTests::randomPartition);
    }

    private static List<Order> randomOrders() {
        return randomList(1, 10, OrderSerializationTests::randomOrder);
    }

    @Override
    protected TopN createTestInstance() {
        return randomTopN(0);
    }

    @Override
    protected TopN mutateInstance(TopN instance) throws IOException {
        Source source = instance.source();
        LogicalPlan child = instance.child();
        List<Partition> partition = instance.partition();
        List<Order> order = instance.order();
        Expression limit = instance.limit();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> partition = randomValueOtherThan(partition, TopNSerializationTests::randomPartitions);
            case 2 -> order = randomValueOtherThan(order, TopNSerializationTests::randomOrders);
            case 3 -> limit = randomValueOtherThan(limit, AbstractExpressionSerializationTests::randomChild);
        }
        return new TopN(source, child, partition, order, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
