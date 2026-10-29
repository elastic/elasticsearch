/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class HistogramFractionSerializationTests extends AbstractExpressionSerializationTests<HistogramFraction> {

    @Override
    protected HistogramFraction createTestInstance() {
        return new HistogramFraction(randomSource(), randomChild(), randomChild(), randomOptionalChild());
    }

    @Override
    protected HistogramFraction mutateInstance(HistogramFraction instance) throws IOException {
        Expression histogram = instance.histogram();
        Expression bucket = instance.bucket();
        Expression decimals = instance.decimals();
        switch (between(0, 2)) {
            case 0 -> histogram = randomValueOtherThan(histogram, AbstractExpressionSerializationTests::randomChild);
            case 1 -> bucket = randomValueOtherThan(bucket, AbstractExpressionSerializationTests::randomChild);
            case 2 -> decimals = randomValueOtherThan(decimals, HistogramFractionSerializationTests::randomOptionalChild);
            default -> throw new AssertionError("unexpected branch");
        }
        return new HistogramFraction(randomSource(), histogram, bucket, decimals);
    }

    private static Expression randomOptionalChild() {
        return randomBoolean() ? null : randomChild();
    }
}
