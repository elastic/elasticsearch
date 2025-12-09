/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class WeightedAvgSerializationTests extends AbstractExpressionSerializationTests<WeightedAvg> {
    @Override
    protected WeightedAvg createTestInstance() {
        return new WeightedAvg(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected WeightedAvg mutateInstance(WeightedAvg instance) throws IOException {
        Expression field = instance.field();
        Expression weight = instance.weight();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            weight = randomValueOtherThan(weight, AbstractExpressionSerializationTests::randomChild);
        }
        return new WeightedAvg(instance.source(), field, weight);
    }
}
