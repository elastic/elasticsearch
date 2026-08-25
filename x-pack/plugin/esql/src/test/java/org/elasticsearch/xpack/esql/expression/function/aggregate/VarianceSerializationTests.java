/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class VarianceSerializationTests extends AbstractExpressionSerializationTests<Variance> {
    @Override
    protected Variance createTestInstance() {
        return new Variance(randomSource(), randomChild(), randomChild(), randomChild(), randomBoolean());
    }

    /**
     * {@code createTestInstance} randomizes the non-finite flag, so a single generic round-trip only exercises one of
     * the two forms; this covers both.
     */
    public void testAllowNonFiniteSurvivesCurrentVersionRoundTrip() throws IOException {
        Variance lenient = new Variance(randomSource(), randomChild(), randomChild(), randomChild(), true);
        assertTrue(copyInstance(lenient).allowNonFinite());

        Variance strict = new Variance(randomSource(), randomChild(), randomChild(), randomChild(), false);
        assertFalse(copyInstance(strict).allowNonFinite());
    }

    @Override
    protected Variance mutateInstance(Variance instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression filter = instance.filter();
        Expression window = instance.window();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 2 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
        }
        return new Variance(source, field, filter, window);
    }
}
