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

import java.io.IOException;

public class LimitExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LimitExec> {
    public static LimitExec randomLimitExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression limit = randomLimit();
        return new LimitExec(source, child, limit, randomEstimatedRowSize());
    }

    private static Expression randomLimit() {
        return new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER);
    }

    @Override
    protected LimitExec createTestInstance() {
        return randomLimitExec(0);
    }

    @Override
    protected LimitExec mutateInstance(LimitExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression limit = instance.limit();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> limit = randomValueOtherThan(limit, LimitExecSerializationTests::randomLimit);
            case 2 -> estimatedRowSize = randomValueOtherThan(estimatedRowSize, LimitExecSerializationTests::randomEstimatedRowSize);
            default -> throw new AssertionError("Unexpected case");
        }
        return new LimitExec(instance.source(), child, limit, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
