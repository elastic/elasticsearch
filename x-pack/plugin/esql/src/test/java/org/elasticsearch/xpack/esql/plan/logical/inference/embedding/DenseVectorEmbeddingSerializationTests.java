/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference.embedding;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class DenseVectorEmbeddingSerializationTests extends AbstractLogicalPlanSerializationTests<DenseVectorEmbedding> {

    @Override
    protected DenseVectorEmbedding createTestInstance() {
        return new DenseVectorEmbedding(
            randomSource(),
            randomChild(0),
            randomInferenceId(),
            randomDimensions(),
            randomInput(),
            randomTargetField()
        );
    }

    @Override
    protected DenseVectorEmbedding mutateInstance(DenseVectorEmbedding instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression dimensions = instance.dimensions();
        Expression input = instance.input();
        Attribute targetField = instance.embeddingField();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> dimensions = randomValueOtherThan(instance.dimensions(), this::randomDimensions);
            case 3 -> input = randomValueOtherThan(input, this::randomInput);
            case 4 -> targetField = randomValueOtherThan(targetField, this::randomTargetField);
        }
        return new DenseVectorEmbedding(instance.source(), child, inferenceId, dimensions, input, targetField);
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
