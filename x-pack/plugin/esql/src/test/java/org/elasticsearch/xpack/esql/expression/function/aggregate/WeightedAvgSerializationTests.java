/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class WeightedAvgSerializationTests extends AbstractExpressionSerializationTests<WeightedAvg> {
    @Override
    protected WeightedAvg createTestInstance() {
        return new WeightedAvg(randomSource(), randomChild(), randomChild(), configuration());
    }

    @Override
    protected WeightedAvg mutateInstance(WeightedAvg instance) throws IOException {
        return new WeightedAvg(
            instance.source(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(instance.weight(), AbstractExpressionSerializationTests::randomChild),
            instance.configuration()
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
