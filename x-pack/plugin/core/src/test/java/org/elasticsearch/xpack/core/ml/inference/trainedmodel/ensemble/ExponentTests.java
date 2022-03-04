/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;

public class ExponentTests extends WeightedAggregatorTests<Exponent> {

    @Override
    Exponent createTestInstance(int numberOfWeights) {
        double[] weights = Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).mapToDouble(Double::valueOf).toArray();
        return new Exponent(weights);
    }

    @Override
    protected Exponent doParseInstance(XContentParser parser) throws IOException {
        return lenient ? Exponent.fromXContentLenient(parser) : Exponent.fromXContentStrict(parser);
    }

    @Override
    protected Exponent createTestInstance() {
        return randomBoolean() ? new Exponent() : createTestInstance(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<Exponent> instanceReader() {
        return Exponent::new;
    }

    public void testAggregate() {
        double[] ones = new double[] { 1.0, 1.0, 1.0, 1.0, 1.0 };
        double[][] values = new double[][] {
            new double[] { .01 },
            new double[] { .2 },
            new double[] { .002 },
            new double[] { -.01 },
            new double[] { .1 } };

        Exponent exponent = new Exponent(ones);
        assertThat(exponent.aggregate(exponent.processValues(values)), closeTo(1.35256, 0.00001));

        double[] variedWeights = new double[] { .01, -1.0, .1, 0.0, 0.0 };

        exponent = new Exponent(variedWeights);
        assertThat(exponent.aggregate(exponent.processValues(values)), closeTo(0.81897, 0.00001));

        exponent = new Exponent();
        assertThat(exponent.aggregate(exponent.processValues(values)), closeTo(1.35256, 0.00001));
    }

    public void testCompatibleWith() {
        Exponent exponent = createTestInstance();
        assertThat(exponent.compatibleWith(TargetType.CLASSIFICATION), is(false));
        assertThat(exponent.compatibleWith(TargetType.REGRESSION), is(true));
    }
}
