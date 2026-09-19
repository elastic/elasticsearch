/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.FieldAttributeTestUtils.createFieldAttribute;

public class LimitRatioBySerializationTests extends AbstractLogicalPlanSerializationTests<LimitRatioBy> {

    @Override
    protected LimitRatioBy createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Expression ratio = randomRatio();
        List<Expression> groupings = randomGroupings();
        return new LimitRatioBy(source, child, ratio, groupings);
    }

    @Override
    protected LimitRatioBy mutateInstance(LimitRatioBy instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression ratio = instance.ratio();
        List<Expression> groupings = instance.groupings();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> ratio = randomValueOtherThan(ratio, LimitRatioBySerializationTests::randomRatio);
            case 2 -> groupings = randomValueOtherThan(groupings, LimitRatioBySerializationTests::randomGroupings);
            default -> throw new IllegalStateException("Should never reach here");
        }
        return new LimitRatioBy(instance.source(), child, ratio, groupings);
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
