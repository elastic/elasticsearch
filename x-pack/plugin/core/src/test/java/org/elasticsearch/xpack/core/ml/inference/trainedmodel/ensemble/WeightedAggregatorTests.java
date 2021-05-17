/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public abstract class WeightedAggregatorTests<T extends OutputAggregator> extends AbstractSerializingTestCase<T> {

    protected boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    public void testWithNullValues() {
        OutputAggregator outputAggregator = createTestInstance();
        NullPointerException ex = expectThrows(NullPointerException.class, () -> outputAggregator.processValues(null));
        assertThat(ex.getMessage(), equalTo("values must not be null"));
    }

    public void testWithValuesOfWrongLength() {
        int numberOfValues = randomIntBetween(5, 10);
        double[][] values = new double[numberOfValues][];
        for (int i = 0; i < numberOfValues; i++) {
            values[i] = new double[] {randomDouble()};
        }

        OutputAggregator outputAggregatorWithTooFewWeights = createTestInstance(randomIntBetween(1, numberOfValues - 1));
        expectThrows(IllegalArgumentException.class, () -> outputAggregatorWithTooFewWeights.processValues(values));

        OutputAggregator outputAggregatorWithTooManyWeights = createTestInstance(randomIntBetween(numberOfValues + 1, numberOfValues + 10));
        expectThrows(IllegalArgumentException.class, () -> outputAggregatorWithTooManyWeights.processValues(values));
    }

    abstract T createTestInstance(int numberOfWeights);
}
