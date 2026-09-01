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
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.FieldAttributeTestUtils.createFieldAttribute;

public class LimitRatioByExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LimitRatioByExec> {

    public static LimitRatioByExec randomLimitRatioByExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression ratio = randomRatio();
        List<Expression> groupings = randomGroupings();
        return new LimitRatioByExec(source, child, ratio, groupings, randomEstimatedRowSize());
    }

    @Override
    protected LimitRatioByExec createTestInstance() {
        return randomLimitRatioByExec(0);
    }

    @Override
    protected LimitRatioByExec mutateInstance(LimitRatioByExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression ratio = instance.ratio();
        List<Expression> groupings = instance.groupings();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> ratio = randomValueOtherThan(ratio, LimitRatioByExecSerializationTests::randomRatio);
            case 2 -> groupings = randomValueOtherThan(groupings, LimitRatioByExecSerializationTests::randomGroupings);
            case 3 -> estimatedRowSize = randomValueOtherThan(estimatedRowSize, LimitRatioByExecSerializationTests::randomEstimatedRowSize);
            default -> throw new AssertionError("Unexpected case");
        }
        return new LimitRatioByExec(instance.source(), child, ratio, groupings, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    private static Expression randomRatio() {
        return new Literal(randomSource(), randomDoubleBetween(0.0, 1.0, true), DataType.DOUBLE);
    }

    private static List<Expression> randomGroupings() {
        return randomList(1, 3, () -> createFieldAttribute(0, false));
    }
}
