/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class StdDevSerializationTests extends AbstractExpressionSerializationTests<StdDev> {
    @Override
    protected StdDev createTestInstance() {
        return new StdDev(randomSource(), randomChild(), randomChild(), randomChild(), randomBoolean());
    }

    /**
     * {@code createTestInstance} randomizes the non-finite flag, so a single generic round-trip only exercises one of
     * the two forms; this covers both.
     */
    public void testAllowNonFiniteSurvivesCurrentVersionRoundTrip() throws IOException {
        StdDev lenient = new StdDev(randomSource(), randomChild(), randomChild(), randomChild(), true);
        assertTrue(copyInstance(lenient).allowNonFinite());

        StdDev strict = new StdDev(randomSource(), randomChild(), randomChild(), randomChild(), false);
        assertFalse(copyInstance(strict).allowNonFinite());
    }

    @Override
    protected StdDev mutateInstance(StdDev instance) throws IOException {
        return new StdDev(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
