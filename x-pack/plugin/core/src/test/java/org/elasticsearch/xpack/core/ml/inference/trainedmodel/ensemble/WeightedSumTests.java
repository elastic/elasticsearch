/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class WeightedSumTests extends WeightedAggregatorTests<WeightedSum> {

    @Override
    WeightedSum createTestInstance(int numberOfWeights) {
        List<Double> weights = new ArrayList<>(numberOfWeights);
        for (int i = 0; i < numberOfWeights; i++) {
            weights.add(randomDouble());
        }
        return new WeightedSum(weights);
    }

    @Override
    protected WeightedSum doParseInstance(XContentParser parser) throws IOException {
        return lenient ? WeightedSum.fromXContentLenient(parser) : WeightedSum.fromXContentStrict(parser);
    }

    @Override
    protected WeightedSum createTestInstance() {
        return createTestInstance(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<WeightedSum> instanceReader() {
        return WeightedSum::new;
    }

    public void testAggregate() {
        List<Double> ones = Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0);
        List<Double> values = Arrays.asList(1.0, 2.0, 2.0, 3.0, 5.0);

        WeightedSum weightedSum = new WeightedSum(ones);
        assertThat(weightedSum.aggregate(weightedSum.processValues(values)), equalTo(13.0));

        List<Double> variedWeights = Arrays.asList(1.0, -1.0, .5, 1.0, 5.0);

        weightedSum = new WeightedSum(variedWeights);
        assertThat(weightedSum.aggregate(weightedSum.processValues(values)), equalTo(28.0));
    }
}
