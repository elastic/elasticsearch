/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;

public class CompletionExecSerializationTests extends AbstractPhysicalPlanSerializationTests<CompletionExec> {
    @Override
    protected CompletionExec createTestInstance() {
        return new CompletionExec(
            randomSource(),
            randomChild(0),
            randomInferenceId(),
            randomPrompt(),
            randomAttribute(),
            randomTaskSettings()
        );
    }

    @Override
    protected CompletionExec mutateInstance(CompletionExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression prompt = instance.prompt();
        Attribute targetField = instance.targetField();
        MapExpression taskSettings = instance.taskSettings();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> prompt = randomValueOtherThan(prompt, this::randomPrompt);
            case 3 -> targetField = randomValueOtherThan(targetField, this::randomAttribute);
            case 4 -> taskSettings = randomValueOtherThan(taskSettings, this::randomTaskSettings);
        }
        return new CompletionExec(instance.source(), child, inferenceId, prompt, targetField, taskSettings);
    }

    private Literal randomInferenceId() {
        return Literal.keyword(Source.EMPTY, randomIdentifier());
    }

    private Expression randomPrompt() {
        return randomBoolean() ? Literal.keyword(Source.EMPTY, randomIdentifier()) : randomAttribute();
    }

    private Attribute randomAttribute() {
        return ReferenceAttributeTests.randomReferenceAttribute(randomBoolean());
    }

    private MapExpression randomTaskSettings() {
        return randomBoolean()
            ? new MapExpression(Source.EMPTY, List.of())
            : new MapExpression(
                Source.EMPTY,
                List.of(Literal.keyword(Source.EMPTY, randomIdentifier()), Literal.fromDouble(Source.EMPTY, randomDouble()))
            );
    }
}
