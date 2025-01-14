/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;

public class RerankSerializationTests extends AbstractLogicalPlanSerializationTests<Rerank> {
    @Override
    protected Rerank createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        return new Rerank(source, child, randomQueryText(), randomInput(), randomInferenceId(), randomWindowSize());
    }

    @Override
    protected Rerank mutateInstance(Rerank instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression queryText = instance.queryText();
        Expression input = instance.input();
        Expression inferenceId = instance.inferenceId();
        Expression windowSize = instance.windowSize();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> queryText = randomValueOtherThan(queryText, this::randomQueryText);
            case 2 -> input = randomValueOtherThan(input, this::randomInput);
            case 3 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 4 -> windowSize = randomValueOtherThan(windowSize, this::randomWindowSize);
        }
        return new Rerank(instance.source(), child, queryText, input, inferenceId, windowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    private Expression randomQueryText() {
        return new Literal(Source.EMPTY, randomIdentifier(), DataType.TEXT);
    }

    private Expression randomInput() {
        return FieldAttributeTests.createFieldAttribute(0, randomBoolean());
    }

    private Expression randomInferenceId() {
        return new Literal(Source.EMPTY, randomIdentifier(), DataType.KEYWORD);
    }

    private Expression randomWindowSize() {
        return new Literal(Source.EMPTY, randomInt(), DataType.INTEGER);
    }
}
