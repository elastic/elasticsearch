/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class PromqlHistogramFractionSerializationTests extends AbstractExpressionSerializationTests<PromqlHistogramFraction> {
    @Override
    protected PromqlHistogramFraction createTestInstance() {
        return new PromqlHistogramFraction(randomSource(), randomChild(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected PromqlHistogramFraction mutateInstance(PromqlHistogramFraction instance) throws IOException {
        Expression field = instance.field();
        Expression upperBound = instance.upperBound();
        Expression lower = instance.lower();
        Expression upper = instance.upper();
        switch (between(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> upperBound = randomValueOtherThan(upperBound, AbstractExpressionSerializationTests::randomChild);
            case 2 -> lower = randomValueOtherThan(lower, AbstractExpressionSerializationTests::randomChild);
            case 3 -> upper = randomValueOtherThan(upper, AbstractExpressionSerializationTests::randomChild);
            default -> throw new AssertionError();
        }
        return new PromqlHistogramFraction(instance.source(), field, upperBound, lower, upper);
    }
}
