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
import static org.hamcrest.Matchers.equalTo;

public class WeightedModeTests extends WeightedAggregatorTests<WeightedMode> {

    @Override
    WeightedMode createTestInstance(int numberOfWeights) {
        double[] weights = Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).mapToDouble(Double::valueOf).toArray();
        return new WeightedMode(weights, randomIntBetween(2, 10));
    }

    @Override
    protected WeightedMode doParseInstance(XContentParser parser) throws IOException {
        return lenient ? WeightedMode.fromXContentLenient(parser) : WeightedMode.fromXContentStrict(parser);
    }

    @Override
    protected WeightedMode createTestInstance() {
        return randomBoolean() ? new WeightedMode(randomIntBetween(2, 10)) : createTestInstance(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<WeightedMode> instanceReader() {
        return WeightedMode::new;
    }

    public void testAggregate() {
        double[] ones = new double[] { 1.0, 1.0, 1.0, 1.0, 1.0 };
        double[][] values = new double[][] {
            new double[] { 1.0 },
            new double[] { 2.0 },
            new double[] { 2.0 },
            new double[] { 3.0 },
            new double[] { 5.0 } };

        WeightedMode weightedMode = new WeightedMode(ones, 6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

        double[] variedWeights = new double[] { 1.0, -1.0, .5, 1.0, 5.0 };

        weightedMode = new WeightedMode(variedWeights, 6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(5.0));

        weightedMode = new WeightedMode(6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

        values = new double[][] {
            new double[] { 1.0 },
            new double[] { 1.0 },
            new double[] { 1.0 },
            new double[] { 1.0 },
            new double[] { 2.0 } };
        weightedMode = new WeightedMode(6);
        double[] processedValues = weightedMode.processValues(values);
        assertThat(processedValues.length, equalTo(6));
        assertThat(processedValues[0], equalTo(0.0));
        assertThat(processedValues[1], closeTo(0.95257412, 0.00001));
        assertThat(processedValues[2], closeTo((1.0 - 0.95257412), 0.00001));
        assertThat(processedValues[3], equalTo(0.0));
        assertThat(processedValues[4], equalTo(0.0));
        assertThat(processedValues[5], equalTo(0.0));
        assertThat(weightedMode.aggregate(processedValues), equalTo(1.0));
    }

    public void testAggregateMultiValueArrays() {
        double[] ones = new double[] { 1.0, 1.0, 1.0, 1.0, 1.0 };
        double[][] values = new double[][] {
            new double[] { 1.0, 0.0, 1.0 },
            new double[] { 2.0, 0.0, 0.0 },
            new double[] { 2.0, 3.0, 1.0 },
            new double[] { 3.0, 3.0, 1.0 },
            new double[] { 1.0, 1.0, 5.0 } };

        WeightedMode weightedMode = new WeightedMode(ones, 3);
        double[] processedValues = weightedMode.processValues(values);
        assertThat(processedValues.length, equalTo(3));
        assertThat(processedValues[0], closeTo(0.665240955, 0.00001));
        assertThat(processedValues[1], closeTo(0.090030573, 0.00001));
        assertThat(processedValues[2], closeTo(0.244728471, 0.00001));
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(0.0));

        double[] variedWeights = new double[] { 1.0, -1.0, .5, 1.0, 5.0 };

        weightedMode = new WeightedMode(variedWeights, 3);
        processedValues = weightedMode.processValues(values);
        assertThat(processedValues.length, equalTo(3));
        assertThat(processedValues[0], closeTo(0.0, 0.00001));
        assertThat(processedValues[1], closeTo(0.0, 0.00001));
        assertThat(processedValues[2], closeTo(0.9999999, 0.00001));
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

    }

    public void testCompatibleWith() {
        WeightedMode weightedMode = createTestInstance();
        assertThat(weightedMode.compatibleWith(TargetType.CLASSIFICATION), is(true));
        assertThat(weightedMode.compatibleWith(TargetType.REGRESSION), is(false));
    }
}
