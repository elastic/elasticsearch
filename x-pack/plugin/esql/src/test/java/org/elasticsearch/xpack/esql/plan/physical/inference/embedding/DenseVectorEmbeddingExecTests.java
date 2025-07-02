/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference.embedding;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class DenseVectorEmbeddingExecTests extends AbstractPhysicalPlanSerializationTests<DenseVectorEmbeddingExec> {

    @Override
    protected DenseVectorEmbeddingExec createTestInstance() {
        return new DenseVectorEmbeddingExec(
            randomSource(),
            randomChild(0),
            randomInferenceId(),
            randomDimensions(),
            randomInput(),
            randomTargetField()
        );
    }

    @Override
    protected DenseVectorEmbeddingExec mutateInstance(DenseVectorEmbeddingExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression dimensions = instance.dimensions();
        Expression input = instance.input();
        Attribute targetField = instance.targetField();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> dimensions = randomValueOtherThan(input, this::randomAttribute);
            case 3 -> input = randomValueOtherThan(input, this::randomInput);
            case 4 -> targetField = randomValueOtherThan(targetField, this::randomTargetField);
        }
        return new DenseVectorEmbeddingExec(instance.source(), child, inferenceId, dimensions, input, targetField);
    }

    private Literal randomInferenceId() {
        return Literal.keyword(EMPTY, randomIdentifier());
    }

    private Expression randomInput() {
        return randomBoolean() ? Literal.keyword(EMPTY, randomIdentifier()) : randomAttribute();
    }

    private Attribute randomTargetField() {
        return ReferenceAttributeTests.randomReferenceAttribute(randomBoolean());
    }

    private Attribute randomAttribute() {
        return ReferenceAttributeTests.randomReferenceAttribute(randomBoolean());
    }

    private Expression randomDimensions() {
        return new Literal(EMPTY, randomInt(), DataType.INTEGER);
    }
}
