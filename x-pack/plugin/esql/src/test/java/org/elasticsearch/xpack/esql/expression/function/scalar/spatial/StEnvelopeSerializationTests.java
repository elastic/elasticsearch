/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StEnvelopeSerializationTests extends AbstractExpressionSerializationTests<StEnvelope> {
    @Override
    protected StEnvelope createTestInstance() {
        return new StEnvelope(randomSource(), randomChild());
    }

    @Override
    protected StEnvelope mutateInstance(StEnvelope instance) throws IOException {
        return new StEnvelope(
            instance.source(),
            randomValueOtherThan(instance.spatialField(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
