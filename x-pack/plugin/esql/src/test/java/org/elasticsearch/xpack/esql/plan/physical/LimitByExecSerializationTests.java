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
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

public class LimitByExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LimitByExec> {

    public static LimitByExec randomLimitByExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression limitPerGroup = randomLimitPerGroup();
        List<Expression> groupings = randomGroupings();
        return new LimitByExec(source, child, limitPerGroup, groupings, randomEstimatedRowSize());
    }

    private static Expression randomLimitPerGroup() {
        return new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER);
    }

    private static List<Expression> randomGroupings() {
        return randomList(1, 3, () -> FieldAttributeTests.createFieldAttribute(0, false));
    }

    @Override
    protected LimitByExec createTestInstance() {
        return randomLimitByExec(0);
    }

    @Override
    protected LimitByExec mutateInstance(LimitByExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression limitPerGroup = instance.limitPerGroup();
        List<Expression> groupings = instance.groupings();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> limitPerGroup = randomValueOtherThan(limitPerGroup, LimitByExecSerializationTests::randomLimitPerGroup);
            case 2 -> groupings = randomValueOtherThan(groupings, LimitByExecSerializationTests::randomGroupings);
            case 3 -> estimatedRowSize = randomValueOtherThan(estimatedRowSize, LimitByExecSerializationTests::randomEstimatedRowSize);
            default -> throw new AssertionError("Unexpected case");
        }
        return new LimitByExec(instance.source(), child, limitPerGroup, groupings, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
