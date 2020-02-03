/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
        double[] ones = new double[]{1.0, 1.0, 1.0, 1.0, 1.0};
        List<Double> values = Arrays.asList(1.0, 2.0, 2.0, 3.0, 5.0);

        WeightedMode weightedMode = new WeightedMode(ones, 6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

        double[] variedWeights = new double[]{1.0, -1.0, .5, 1.0, 5.0};

        weightedMode = new WeightedMode(variedWeights, 6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(5.0));

        weightedMode = new WeightedMode(6);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

        values = Arrays.asList(1.0, 1.0, 1.0, 1.0, 2.0);
        weightedMode = new WeightedMode(6);
        List<Double> processedValues = weightedMode.processValues(values);
        assertThat(processedValues.size(), equalTo(6));
        assertThat(processedValues.get(0), equalTo(0.0));
        assertThat(processedValues.get(1), closeTo(0.95257412, 0.00001));
        assertThat(processedValues.get(2), closeTo((1.0 - 0.95257412), 0.00001));
        assertThat(processedValues.get(3), equalTo(0.0));
        assertThat(processedValues.get(4), equalTo(0.0));
        assertThat(processedValues.get(5), equalTo(0.0));
        assertThat(weightedMode.aggregate(processedValues), equalTo(1.0));
    }

    public void testCompatibleWith() {
        WeightedMode weightedMode = createTestInstance();
        assertThat(weightedMode.compatibleWith(TargetType.CLASSIFICATION), is(true));
        assertThat(weightedMode.compatibleWith(TargetType.REGRESSION), is(false));
    }
}
