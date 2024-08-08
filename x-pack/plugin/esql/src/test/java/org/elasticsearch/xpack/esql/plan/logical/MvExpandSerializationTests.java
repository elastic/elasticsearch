/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;

import java.io.IOException;

public class MvExpandSerializationTests extends AbstractLogicalPlanSerializationTests<MvExpand> {
    @Override
    protected MvExpand createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        NamedExpression target = FieldAttributeTests.createFieldAttribute(0, false);
        Attribute expanded = ReferenceAttributeTests.randomReferenceAttribute();
        return new MvExpand(source, child, target, expanded);
    }

    @Override
    protected MvExpand mutateInstance(MvExpand instance) throws IOException {
        LogicalPlan child = instance.child();
        NamedExpression target = instance.target();
        Attribute expanded = instance.expanded();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> target = randomValueOtherThan(target, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 2 -> expanded = randomValueOtherThan(expanded, ReferenceAttributeTests::randomReferenceAttribute);
        }
        return new MvExpand(instance.source(), child, target, expanded);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
