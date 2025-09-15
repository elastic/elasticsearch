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

public class LimitSerializationTests extends AbstractLogicalPlanSerializationTests<Limit> {
    @Override
    protected Limit createTestInstance() {
        Source source = randomSource();
        Expression limit = FieldAttributeTests.createFieldAttribute(0, false);
        LogicalPlan child = randomChild(0);
        return new Limit(source, limit, child, randomBoolean());
    }

    @Override
    protected Limit mutateInstance(Limit instance) throws IOException {
        Expression limit = instance.limit();
        LogicalPlan child = instance.child();
        boolean duplicated = instance.duplicated();
        switch (randomIntBetween(0, 2)) {
            case 0 -> limit = randomValueOtherThan(limit, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 1 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 2 -> duplicated = duplicated == false;
            default -> throw new IllegalStateException("Should never reach here");
        }
        return new Limit(instance.source(), limit, child, duplicated);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected Limit copyInstance(Limit instance, TransportVersion version) throws IOException {
        // Limit#duplicated() is ALWAYS false when being serialized and we assert that in Limit#writeTo().
        // So, we need to manually simulate this situation.
        Limit deserializedCopy = super.copyInstance(instance.withDuplicated(false), version);
        return deserializedCopy.withDuplicated(instance.duplicated());
    }
}
