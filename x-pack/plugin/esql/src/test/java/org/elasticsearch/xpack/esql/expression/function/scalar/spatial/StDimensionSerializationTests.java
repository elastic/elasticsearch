/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StDimensionSerializationTests extends AbstractExpressionSerializationTests<StDimension> {
    @Override
    protected StDimension createTestInstance() {
        return new StDimension(randomSource(), randomChild());
    }

    @Override
    protected StDimension mutateInstance(StDimension instance) throws IOException {
        return new StDimension(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
