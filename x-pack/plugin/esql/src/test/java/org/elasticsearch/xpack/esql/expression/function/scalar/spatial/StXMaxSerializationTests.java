/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StXMaxSerializationTests extends AbstractExpressionSerializationTests<StXMax> {
    @Override
    protected StXMax createTestInstance() {
        return new StXMax(randomSource(), randomChild());
    }

    @Override
    protected StXMax mutateInstance(StXMax instance) throws IOException {
        return new StXMax(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
