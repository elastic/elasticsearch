/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;

public class CompletionSerializationTests extends AbstractLogicalPlanSerializationTests<Completion> {

    @Override
    protected Completion createTestInstance() {
        return new Completion(randomSource(), randomChild(0), randomInferenceId(), randomPrompt(), randomAttribute());
    }

    @Override
    protected Completion mutateInstance(Completion instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression prompt = instance.prompt();
        Attribute targetField = instance.targetField();

        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> prompt = randomValueOtherThan(prompt, this::randomPrompt);
            case 3 -> targetField = randomValueOtherThan(targetField, this::randomAttribute);
        }
        return new Completion(instance.source(), child, inferenceId, prompt, targetField);
    }

    private Literal randomInferenceId() {
        return new Literal(Source.EMPTY, randomIdentifier(), DataType.KEYWORD);
    }

    private Expression randomPrompt() {
        return randomBoolean() ? new Literal(Source.EMPTY, randomIdentifier(), DataType.KEYWORD) : randomAttribute();
    }

    private Attribute randomAttribute() {
        return ReferenceAttributeTests.randomReferenceAttribute(randomBoolean());
    }
}
