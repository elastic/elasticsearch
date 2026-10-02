/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StXMinSerializationTests extends AbstractExpressionSerializationTests<StXMin> {
    @Override
    protected StXMin createTestInstance() {
        return new StXMin(randomSource(), randomChild());
    }

    @Override
    protected StXMin mutateInstance(StXMin instance) throws IOException {
        return new StXMin(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
