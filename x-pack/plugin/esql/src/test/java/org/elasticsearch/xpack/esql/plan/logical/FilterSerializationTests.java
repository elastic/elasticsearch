/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;

public class FilterSerializationTests extends AbstractLogicalPlanSerializationTests<Filter> {
    @Override
    protected Filter createTestInstance() {
        LogicalPlan child = randomChild(0);
        Expression condition = FieldAttributeTests.createFieldAttribute(3, false);
        return new Filter(randomSource(), child, condition);
    }

    @Override
    protected Filter mutateInstance(Filter instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression condition = instance.condition();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            condition = randomValueOtherThan(condition, () -> FieldAttributeTests.createFieldAttribute(3, false));
        }
        return new Filter(instance.source(), child, condition);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
