/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;

import java.io.IOException;
import java.util.List;

public class GrokSerializationTests extends AbstractLogicalPlanSerializationTests<Grok> {
    @Override
    protected Grok createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Expression inputExpr = FieldAttributeTests.createFieldAttribute(3, false);
        String pattern = randomAlphaOfLength(5);
        List<Attribute> extracted = randomList(1, 10, ReferenceAttributeTests::randomReferenceAttribute);
        return new Grok(source, child, inputExpr, Grok.pattern(source, pattern), extracted);
    }

    @Override
    protected Grok mutateInstance(Grok instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression inputExpr = instance.input();
        String pattern = instance.parser().pattern();
        List<Attribute> extracted = instance.extractedFields();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inputExpr = randomValueOtherThan(inputExpr, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 2 -> pattern = randomValueOtherThan(pattern, () -> randomAlphaOfLength(5));
        }
        return new Grok(instance.source(), child, inputExpr, Grok.pattern(instance.source(), pattern), extracted);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
