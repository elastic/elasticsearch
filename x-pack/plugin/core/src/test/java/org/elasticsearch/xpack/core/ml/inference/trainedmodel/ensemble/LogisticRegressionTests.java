/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
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
        double[][] values = new double[][]{
            new double[] {1.0},
            new double[] {2.0},
            new double[] {2.0},
            new double[] {3.0},
            new double[] {5.0}
        };

        LogisticRegression logisticRegression = new LogisticRegression(ones);
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(1.0));

        double[] variedWeights = new double[]{.01, -1.0, .1, 0.0, 0.0};

        logisticRegression = new LogisticRegression(variedWeights);
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(0.0));

        logisticRegression = new LogisticRegression();
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(1.0));
    }

    public void testAggregateMultiValueArrays() {
        double[] ones = new double[]{1.0, 1.0, 1.0, 1.0, 1.0};
        double[][] values = new double[][]{
            new double[] {1.0, 0.0, 1.0},
            new double[] {2.0, 0.0, 0.0},
            new double[] {2.0, 3.0, 1.0},
            new double[] {3.0, 3.0, 1.0},
            new double[] {1.0, 1.0, 5.0}
        };

        LogisticRegression logisticRegression = new LogisticRegression(ones);
        double[] processedValues = logisticRegression.processValues(values);
        assertThat(processedValues.length, equalTo(3));
        assertThat(processedValues[0], closeTo(0.665240955, 0.00001));
        assertThat(processedValues[1], closeTo(0.090030573, 0.00001));
        assertThat(processedValues[2], closeTo(0.244728471, 0.00001));
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(0.0));

        double[] variedWeights = new double[]{1.0, -1.0, .5, 1.0, 5.0};

        logisticRegression = new LogisticRegression(variedWeights);
        processedValues = logisticRegression.processValues(values);
        assertThat(processedValues.length, equalTo(3));
        assertThat(processedValues[0], closeTo(0.0, 0.00001));
        assertThat(processedValues[1], closeTo(0.0, 0.00001));
        assertThat(processedValues[2], closeTo(0.9999999, 0.00001));
        assertThat(logisticRegression.aggregate(logisticRegression.processValues(values)), equalTo(2.0));

    }

    public void testCompatibleWith() {
        LogisticRegression logisticRegression = createTestInstance();
        assertThat(logisticRegression.compatibleWith(TargetType.CLASSIFICATION), is(true));
        assertThat(logisticRegression.compatibleWith(TargetType.REGRESSION), is(true));
    }
}
