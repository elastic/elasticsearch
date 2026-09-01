/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.equalTo;

public class DenseVectorExecSerializationTests extends AbstractPhysicalPlanSerializationTests<DenseVectorExec> {

    @Override
    protected DenseVectorExec createTestInstance() {
        return new DenseVectorExec(
            randomSource(),
            randomChild(0),
            randomInferenceId(),
            randomFields(),
            randomGeneratedFields(),
            randomTimeout(),
            randomInputType(),
            randomEndpointTaskType()
        );
    }

    @Override
    protected DenseVectorExec mutateInstance(DenseVectorExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        List<NamedExpression> fields = instance.fields();
        List<Attribute> generatedFields = instance.generatedFields();
        TimeValue timeout = instance.timeout();
        org.elasticsearch.inference.DataType inputType = instance.inputType();
        org.elasticsearch.inference.TaskType endpointTaskType = instance.endpointTaskType();

        switch (between(0, 6)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> fields = randomValueOtherThan(fields, this::randomFields);
            case 3 -> generatedFields = randomValueOtherThan(generatedFields, this::randomGeneratedFields);
            case 4 -> timeout = randomValueOtherThan(timeout, this::randomTimeout);
            case 5 -> inputType = randomValueOtherThan(inputType, this::randomInputType);
            case 6 -> endpointTaskType = randomValueOtherThan(endpointTaskType, this::randomEndpointTaskType);
        }
        return new DenseVectorExec(instance.source(), child, inferenceId, fields, generatedFields, timeout, inputType, endpointTaskType);
    }

    /**
     * Plans from a node without the {@code esql_dense_vector_type_option} transport version carry neither the input type
     * nor the endpoint task type. Both must fall back to the text embedding request shape, the only one such a node can
     * describe; a null task type would select the multimodal shape instead.
     */
    public void testOlderTransportVersionMeansTextEmbedding() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(InferencePlan.ESQL_DENSE_VECTOR_TYPE_OPTION);
        DenseVectorExec original = new DenseVectorExec(
            randomSource(),
            new ExchangeSourceExec(randomSource(), List.of(), false),
            randomInferenceId(),
            List.of(),
            List.of(),
            randomTimeout(),
            org.elasticsearch.inference.DataType.IMAGE,
            org.elasticsearch.inference.TaskType.EMBEDDING
        );

        DenseVectorExec roundTripped = copyInstance(original, before);

        assertThat(roundTripped.inputType(), equalTo(org.elasticsearch.inference.DataType.TEXT));
        assertThat(roundTripped.endpointTaskType(), equalTo(org.elasticsearch.inference.TaskType.TEXT_EMBEDDING));
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

    private List<NamedExpression> randomFields() {
        return randomList(0, 5, () -> (NamedExpression) randomReferenceAttribute(randomBoolean()));
    }

    private List<Attribute> randomGeneratedFields() {
        return randomList(0, 5, () -> (Attribute) randomReferenceAttribute(randomBoolean()));
    }

    private TimeValue randomTimeout() {
        return randomBoolean() ? null : TimeValue.timeValueMillis(randomLongBetween(1, 300_000));
    }
}
