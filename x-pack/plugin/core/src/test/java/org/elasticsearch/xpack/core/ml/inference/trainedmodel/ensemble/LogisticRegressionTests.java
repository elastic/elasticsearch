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
import static org.hamcrest.Matchers.equalTo;

public class LogisticRegressionTests extends WeightedAggregatorTests<LogisticRegression> {

    @Override
    LogisticRegression createTestInstance(int numberOfWeights) {
        double[] weights = Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).mapToDouble(Double::valueOf).toArray();
        return new LogisticRegression(weights);
    }

    @Override
    protected LogisticRegression doParseInstance(XContentParser parser) throws IOException {
        return lenient ? LogisticRegression.fromXContentLenient(parser) : LogisticRegression.fromXContentStrict(parser);
    }

    @Override
    protected LogisticRegression createTestInstance() {
        return randomBoolean() ? new LogisticRegression() : createTestInstance(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<LogisticRegression> instanceReader() {
        return LogisticRegression::new;
    }

    public void testAggregate() {
        double[] ones = new double[]{1.0, 1.0, 1.0, 1.0, 1.0};
        List<Double> values = Arrays.asList(1.0, 2.0, 2.0, 3.0, 5.0);

        LogisticRegression logisticRegression = new LogisticRegression(ones);
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(1.0));

        double[] variedWeights = new double[]{.01, -1.0, .1, 0.0, 0.0};

        logisticRegression = new LogisticRegression(variedWeights);
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(0.0));

        logisticRegression = new LogisticRegression();
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(1.0));
    }

    public void testCompatibleWith() {
        LogisticRegression logisticRegression = createTestInstance();
        assertThat(logisticRegression.compatibleWith(TargetType.CLASSIFICATION), is(true));
        assertThat(logisticRegression.compatibleWith(TargetType.REGRESSION), is(true));
    }
}
