/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StYMinSerializationTests extends AbstractExpressionSerializationTests<StYMin> {
    @Override
    protected StYMin createTestInstance() {
        return new StYMin(randomSource(), randomChild());
    }

    @Override
    protected StYMin mutateInstance(StYMin instance) throws IOException {
        return new StYMin(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
