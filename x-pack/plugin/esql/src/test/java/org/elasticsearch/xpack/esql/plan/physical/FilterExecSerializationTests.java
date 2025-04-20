/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;

public class FilterExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FilterExec> {
    public static FilterExec randomFilterExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression condition = FieldAttributeTests.createFieldAttribute(0, false);
        return new FilterExec(source, child, condition);
    }

    @Override
    protected FilterExec createTestInstance() {
        return randomFilterExec(0);
    }

    @Override
    protected FilterExec mutateInstance(FilterExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression condition = instance.condition();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            condition = randomValueOtherThan(condition, () -> FieldAttributeTests.createFieldAttribute(0, false));
        }
        return new FilterExec(instance.source(), child, condition);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
