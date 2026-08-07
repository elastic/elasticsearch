/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class DenseVectorExecSerializationTests extends AbstractPhysicalPlanSerializationTests<DenseVectorExec> {

    @Override
    protected DenseVectorExec createTestInstance() {
        return new DenseVectorExec(
            randomSource(),
            randomChild(0),
            randomInferenceId(),
            randomFields(),
            randomGeneratedFields(),
            randomTimeout()
        );
    }

    @Override
    protected DenseVectorExec mutateInstance(DenseVectorExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inferenceId = instance.inferenceId();
        List<NamedExpression> fields = instance.fields();
        List<Attribute> generatedFields = instance.generatedFields();
        TimeValue timeout = instance.timeout();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, this::randomInferenceId);
            case 2 -> fields = randomValueOtherThan(fields, this::randomFields);
            case 3 -> generatedFields = randomValueOtherThan(generatedFields, this::randomGeneratedFields);
            case 4 -> timeout = randomValueOtherThan(timeout, this::randomTimeout);
        }
        return new DenseVectorExec(instance.source(), child, inferenceId, fields, generatedFields, timeout);
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
