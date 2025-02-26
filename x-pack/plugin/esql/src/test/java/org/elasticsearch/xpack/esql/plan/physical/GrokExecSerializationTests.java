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
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.io.IOException;

public class GrokExecSerializationTests extends AbstractPhysicalPlanSerializationTests<GrokExec> {
    public static GrokExec randomGrokExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression inputExpression = FieldAttributeTests.createFieldAttribute(0, false);
        Grok.Parser parser = Grok.pattern(source, EsqlNodeSubclassTests.randomGrokPattern());
        return new GrokExec(source, child, inputExpression, parser, parser.extractedFields());
    }

    @Override
    protected GrokExec createTestInstance() {
        return randomGrokExec(0);
    }

    @Override
    protected GrokExec mutateInstance(GrokExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inputExpression = instance.inputExpression();
        Grok.Parser parser = instance.pattern();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inputExpression = randomValueOtherThan(inputExpression, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 2 -> parser = Grok.pattern(
                instance.source(),
                randomValueOtherThan(parser.pattern(), EsqlNodeSubclassTests::randomGrokPattern)
            );
        }
        return new GrokExec(instance.source(), child, inputExpression, parser, parser.extractedFields());
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
