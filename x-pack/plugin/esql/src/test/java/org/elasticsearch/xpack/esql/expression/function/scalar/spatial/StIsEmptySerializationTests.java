/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StIsEmptySerializationTests extends AbstractExpressionSerializationTests<StIsEmpty> {
    @Override
    protected StIsEmpty createTestInstance() {
        return new StIsEmpty(randomSource(), randomChild());
    }

    @Override
    protected StIsEmpty mutateInstance(StIsEmpty instance) throws IOException {
        return new StIsEmpty(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
