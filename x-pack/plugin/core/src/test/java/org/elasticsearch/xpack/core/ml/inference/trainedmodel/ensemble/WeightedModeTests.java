/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class WeightedModeTests extends WeightedAggregatorTests<WeightedMode> {

    @Override
    WeightedMode createTestInstance(int numberOfWeights) {
        List<Double> weights = Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).collect(Collectors.toList());
        return new WeightedMode(weights);
    }

    @Override
    protected WeightedMode doParseInstance(XContentParser parser) throws IOException {
        return lenient ? WeightedMode.fromXContentLenient(parser) : WeightedMode.fromXContentStrict(parser);
    }

    @Override
    protected WeightedMode createTestInstance() {
        return randomBoolean() ? new WeightedMode() : createTestInstance(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<WeightedMode> instanceReader() {
        return WeightedMode::new;
    }

    public void testAggregate() {
        List<Double> ones = Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0);
        List<Double> values = Arrays.asList(1.0, 2.0, 2.0, 3.0, 5.0);

        WeightedMode weightedMode = new WeightedMode(ones);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));

        List<Double> variedWeights = Arrays.asList(1.0, -1.0, .5, 1.0, 5.0);

        weightedMode = new WeightedMode(variedWeights);
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(5.0));

        weightedMode = new WeightedMode();
        assertThat(weightedMode.aggregate(weightedMode.processValues(values)), equalTo(2.0));
    }
}
