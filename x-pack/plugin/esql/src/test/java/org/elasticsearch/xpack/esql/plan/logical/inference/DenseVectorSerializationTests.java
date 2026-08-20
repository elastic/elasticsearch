/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class DenseVectorSerializationTests extends AbstractLogicalPlanSerializationTests<DenseVector> {
    @Override
    protected DenseVector createTestInstance() {
        Source source = randomSource();
        List<NamedExpression> fields = randomFields();
        return new DenseVector(
            source,
            randomChild(0),
            randomInferenceId(),
            randomRowLimit(),
            fields,
            DenseVector.generatedAttributesFor(source, fields),
            randomTimeout(),
            randomInputType(),
            randomEndpointTaskType()
        );
    }

    @Override
    protected DenseVector mutateInstance(DenseVector instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        Expression rowLimit = instance.rowLimit();
        List<NamedExpression> fields = instance.fields();
        List<Attribute> generatedFields = instance.generatedAttributes();
        TimeValue timeout = instance.timeout();
        org.elasticsearch.inference.DataType inputType = instance.inputType();
        org.elasticsearch.inference.TaskType endpointTaskType = instance.endpointTaskType();

        switch (between(0, 6)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> rowLimit = randomValueOtherThan(rowLimit, this::randomRowLimit);
            case 3 -> {
                // Keep generatedFields consistent with fields (1:1), as produced during analysis.
                fields = randomValueOtherThan(fields, this::randomFields);
                generatedFields = DenseVector.generatedAttributesFor(instance.source(), fields);
            }
            case 4 -> timeout = randomValueOtherThan(timeout, this::randomTimeout);
            case 5 -> inputType = randomValueOtherThan(inputType, this::randomInputType);
            case 6 -> endpointTaskType = randomValueOtherThan(endpointTaskType, this::randomEndpointTaskType);
        }
        return new DenseVector(
            instance.source(),
            child,
            inferenceId,
            rowLimit,
            fields,
            generatedFields,
            timeout,
            inputType,
            endpointTaskType
        );
    }

    private org.elasticsearch.inference.DataType randomInputType() {
        return randomFrom(org.elasticsearch.inference.DataType.TEXT, org.elasticsearch.inference.DataType.IMAGE);
    }

    private org.elasticsearch.inference.TaskType randomEndpointTaskType() {
        return randomFrom(org.elasticsearch.inference.TaskType.TEXT_EMBEDDING, org.elasticsearch.inference.TaskType.EMBEDDING);
    }

    private Literal randomInferenceId() {
        return Literal.keyword(Source.EMPTY, randomIdentifier());
    }

    private Expression randomRowLimit() {
        return new Literal(Source.EMPTY, randomIntBetween(1, 100), DataType.INTEGER);
    }

    private List<NamedExpression> randomFields() {
        return randomList(0, 5, () -> (NamedExpression) randomReferenceAttribute(randomBoolean()));
    }

    private TimeValue randomTimeout() {
        return randomBoolean() ? null : TimeValue.timeValueMillis(randomLongBetween(1, 300_000));
    }
}
