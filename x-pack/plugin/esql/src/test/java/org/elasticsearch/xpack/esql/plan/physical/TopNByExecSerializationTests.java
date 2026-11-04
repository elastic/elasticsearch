/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator.OutputOrdering;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.OrderSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class TopNByExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TopNByExec> {
    public static TopNByExec randomTopNByExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Order> order = randomList(1, 10, OrderSerializationTests::randomOrder);
        Expression limit = new Literal(randomSource(), randomNonNegativeInt(), DataType.INTEGER);
        Integer estimatedRowSize = randomEstimatedRowSize();
        List<Expression> groupings = randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList();
        return new TopNByExec(source, child, order, limit, groupings, randomBoolean(), estimatedRowSize);
    }

    @Override
    protected TopNByExec createTestInstance() {
        return randomTopNByExec(0);
    }

    @Override
    protected TopNByExec mutateInstance(TopNByExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Order> order = instance.order();
        Expression limit = instance.limitPerGroup();
        Integer estimatedRowSize = instance.estimatedRowSize();
        List<Expression> groupings = instance.groupings();
        boolean withTies = instance.withTies();

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
                () -> randomFieldAttributes(1, 5, false).stream().map(a -> (Expression) a).toList()
            );
            case 5 -> withTies = withTies == false;
        }
        return new TopNByExec(instance.source(), child, order, limit, groupings, withTies, estimatedRowSize);
    }

    public void testBackcompatRejectsTies() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(TopNBy.ESQL_TOPN_BY_WITH_TIES);
        TopNByExec instance = randomTopNByExec(0);
        TopNByExec original = new TopNByExec(
            instance.source(),
            instance.child(),
            instance.order(),
            instance.limitPerGroup(),
            instance.groupings(),
            true,
            instance.estimatedRowSize()
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(original, oldVersion));
        assertThat(e.getMessage(), containsString("SORT | LIMIT BY with ties is not supported in peer node's version [" + oldVersion));
    }

    public void testDeserializeDefaultsOutputOrderingToSorted() throws IOException {
        TopNByExec instance = randomTopNByExec(0).withNonSortedOutput();
        TopNByExec deserialized = copyInstance(instance);
        assertEquals(OutputOrdering.SORTED, deserialized.outputOrdering());
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
