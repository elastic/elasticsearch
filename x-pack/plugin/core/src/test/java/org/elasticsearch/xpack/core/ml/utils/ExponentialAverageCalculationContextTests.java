/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.time.Instant;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExponentialAverageCalculationContextTests extends AbstractSerializingTestCase<ExponentialAverageCalculationContext> {

    public static ExponentialAverageCalculationContext createRandom() {
        return new ExponentialAverageCalculationContext(
            randomDouble(),
            randomBoolean() ? Instant.now() : null,
            randomBoolean() ? randomDouble() : null);
    }

    @Override
    public ExponentialAverageCalculationContext createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<ExponentialAverageCalculationContext> instanceReader() {
        return ExponentialAverageCalculationContext::new;
    }

    @Override
    protected ExponentialAverageCalculationContext doParseInstance(XContentParser parser) {
        return ExponentialAverageCalculationContext.PARSER.apply(parser, null);
    }

    public void testDefaultConstructor() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext();

        assertThat(context.getIncrementalMetricValueMs(), equalTo(0.0));
        assertThat(context.getLatestTimestamp(), nullValue());
        assertThat(context.getPreviousExponentialAverageMs(), nullValue());
    }

    public void testConstructor() {
        ExponentialAverageCalculationContext context =
            new ExponentialAverageCalculationContext(1.23, Instant.ofEpochMilli(123456789), 4.56);

        assertThat(context.getIncrementalMetricValueMs(), equalTo(1.23));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.ofEpochMilli(123456789)));
        assertThat(context.getPreviousExponentialAverageMs(), equalTo(4.56));
    }

    public void testCopyConstructor() {
        ExponentialAverageCalculationContext context1 =
            new ExponentialAverageCalculationContext(1.23, Instant.ofEpochMilli(123456789), 4.56);
        ExponentialAverageCalculationContext context2 = new ExponentialAverageCalculationContext(context1);

        assertThat(context2.getIncrementalMetricValueMs(), equalTo(1.23));
        assertThat(context2.getLatestTimestamp(), equalTo(Instant.ofEpochMilli(123456789)));
        assertThat(context2.getPreviousExponentialAverageMs(), equalTo(4.56));
        assertThat(context2.getCurrentExponentialAverageMs(), equalTo(context1.getCurrentExponentialAverageMs()));
    }

    public void testExponentialAverageCalculation() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(0.0, null, null);
        assertThat(context.getIncrementalMetricValueMs(), equalTo(0.0));
        assertThat(context.getLatestTimestamp(), nullValue());
        assertThat(context.getPreviousExponentialAverageMs(), nullValue());
        assertThat(context.getCurrentExponentialAverageMs(), equalTo(0.0));

        context.increment(100.0);
        context.increment(100.0);
        context.increment(100.0);
        assertThat(context.getIncrementalMetricValueMs(), equalTo(300.0));
        assertThat(context.getLatestTimestamp(), nullValue());
        assertThat(context.getPreviousExponentialAverageMs(), nullValue());
        assertThat(context.getCurrentExponentialAverageMs(), equalTo(300.0));

        context.setLatestTimestamp(Instant.parse("2019-07-19T03:30:00.00Z"));
        assertThat(context.getIncrementalMetricValueMs(), equalTo(300.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T03:30:00.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), nullValue());
        assertThat(context.getCurrentExponentialAverageMs(), equalTo(300.0));

        context.increment(200.0);
        assertThat(context.getIncrementalMetricValueMs(), equalTo(500.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T03:30:00.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), nullValue());
        assertThat(context.getCurrentExponentialAverageMs(), equalTo(500.0));

        context.setLatestTimestamp(Instant.parse("2019-07-19T04:00:01.00Z"));
        assertThat(context.getIncrementalMetricValueMs(), equalTo(0.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T04:00:01.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), equalTo(500.0));
        assertThat(context.getCurrentExponentialAverageMs(), closeTo(499.8, 0.1));

        context.increment(1000.0);
        context.setLatestTimestamp(Instant.parse("2019-07-19T04:30:00.00Z"));
        assertThat(context.getIncrementalMetricValueMs(), equalTo(1000.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T04:30:00.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), equalTo(500.0));
        assertThat(context.getCurrentExponentialAverageMs(), closeTo(696.7, 0.1));
    }

    public void testExponentialAverageCalculationOnWindowBoundary() {
        ExponentialAverageCalculationContext context =
            new ExponentialAverageCalculationContext(500.0, Instant.parse("2019-07-19T04:25:06.00Z"), 200.0);
        assertThat(context.getIncrementalMetricValueMs(), equalTo(500.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T04:25:06.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), equalTo(200.0));
        assertThat(context.getCurrentExponentialAverageMs(), closeTo(302.5, 0.1));

        context.setLatestTimestamp(Instant.parse("2019-07-19T05:00:00.00Z"));
        assertThat(context.getIncrementalMetricValueMs(), equalTo(0.0));
        assertThat(context.getLatestTimestamp(), equalTo(Instant.parse("2019-07-19T05:00:00.00Z")));
        assertThat(context.getPreviousExponentialAverageMs(), closeTo(302.5, 0.1));
        assertThat(context.getCurrentExponentialAverageMs(), equalTo(context.getPreviousExponentialAverageMs()));
    }
}
