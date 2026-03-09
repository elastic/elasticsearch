/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.Limit;

import java.io.IOException;
import java.util.List;

public class LimitExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LimitExec> {
    public static LimitExec randomLimitExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression limit = randomLimit();
        return new LimitExec(source, child, limit, randomGroupings(), randomEstimatedRowSize());
    }

    private static Expression randomLimit() {
        return new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER);
    }

    private static List<Expression> randomGroupings() {
        return randomList(0, 3, () -> FieldAttributeTests.createFieldAttribute(0, false));
    }

    @Override
    protected LimitExec createTestInstance() {
        return randomLimitExec(0);
    }

    @Override
    protected LimitExec mutateInstance(LimitExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression limit = instance.limit();
        List<Expression> groupings = instance.groupings();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> limit = randomValueOtherThan(limit, LimitExecSerializationTests::randomLimit);
            case 2 -> groupings = randomValueOtherThan(groupings, LimitExecSerializationTests::randomGroupings);
            case 3 -> estimatedRowSize = randomValueOtherThan(estimatedRowSize, LimitExecSerializationTests::randomEstimatedRowSize);
            default -> throw new AssertionError("Unexpected case");
        }
        return new LimitExec(instance.source(), child, limit, groupings, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testSerializationWithGroupingsOnOldVersion() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Limit.ESQL_LIMIT_BY);
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        Expression limit = randomLimit();
        List<Expression> groupings = randomList(1, 3, () -> FieldAttributeTests.createFieldAttribute(0, false));
        LimitExec instance = new LimitExec(source, child, limit, groupings, randomEstimatedRowSize());
        Exception e = expectThrows(IllegalArgumentException.class, () -> copyInstance(instance, oldVersion));
        assertEquals("LIMIT BY is not supported by all nodes in the cluster", e.getMessage());
    }

    public void testSerializationWithoutGroupingsOnOldVersion() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Limit.ESQL_LIMIT_BY);
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        Expression limit = randomLimit();
        LimitExec instance = new LimitExec(source, child, limit, randomEstimatedRowSize());
        LimitExec deserialized = copyInstance(instance, oldVersion);
        assertEquals(List.of(), deserialized.groupings());
    }
}
