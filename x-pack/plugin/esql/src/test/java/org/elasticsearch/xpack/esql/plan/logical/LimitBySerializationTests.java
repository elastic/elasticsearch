/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

public class LimitBySerializationTests extends AbstractLogicalPlanSerializationTests<LimitBy> {
    @Override
    protected LimitBy createTestInstance() {
        Source source = randomSource();
        Expression limit = FieldAttributeTests.createFieldAttribute(0, false);
        LogicalPlan child = randomChild(0);
        List<Expression> groupings = randomGroupings();
        return new LimitBy(source, limit, child, groupings, randomBoolean());
    }

    @Override
    protected LimitBy mutateInstance(LimitBy instance) throws IOException {
        Expression limit = instance.limit();
        LogicalPlan child = instance.child();
        List<Expression> groupings = instance.groupings();
        boolean duplicated = instance.duplicated();
        switch (randomIntBetween(0, 3)) {
            case 0 -> limit = randomValueOtherThan(limit, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 1 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 2 -> groupings = randomValueOtherThan(groupings, LimitBySerializationTests::randomGroupings);
            case 3 -> duplicated = duplicated == false;
            default -> throw new IllegalStateException("Should never reach here");
        }
        return new LimitBy(instance.source(), limit, child, groupings, duplicated);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected LimitBy copyInstance(LimitBy instance, TransportVersion version) throws IOException {
        LimitBy deserializedCopy = super.copyInstance(instance, version);
        return deserializedCopy.withDuplicated(instance.duplicated());
    }

    private static List<Expression> randomGroupings() {
        return randomList(1, 3, () -> FieldAttributeTests.createFieldAttribute(0, false));
    }
}
