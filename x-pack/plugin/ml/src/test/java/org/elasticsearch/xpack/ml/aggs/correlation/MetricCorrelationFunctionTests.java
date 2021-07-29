/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class MetricCorrelationFunctionTests extends ESTestCase {

    public void testWithBadInput() {
        InternalDateRange agg = new InternalDateRange.Factory().create(
            "daily",
            Arrays.asList(
                new InternalDateRange.Bucket(
                    "*-2021-07-19T00:00:00.000Z",
                    Double.NaN,
                    1.6266528E12,
                    0,
                    Collections.singletonList(new InternalAvg("ticketPrice", 0L, 0L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                ),
                new InternalDateRange.Bucket(
                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                    1.6266528E12,
                    1.6266564E12,
                    1,
                    Collections.singletonList(new InternalAvg("ticketPrice", 841.265625, 1L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                )
            ),
            DocValueFormat.RAW,
            true,
            Collections.emptyMap()
        );

        MetricCorrelationFunction metricCorrelationFunction = new MetricCorrelationFunction("daily>_count");
        Exception ex = expectThrows(
            Exception.class,
            () -> metricCorrelationFunction.execute(
                new MlAggsHelper.DoubleBucketValues(new long[] { 1, 1, 1 }, new double[] { 0.4, 0.5, 0.6 }),
                new Aggregations(Collections.singletonList(agg))
            )
        );
        assertThat(ex.getMessage(), containsString("value lengths do not match"));

        InternalDateRange singleBucket = new InternalDateRange.Factory().create(
            "daily",
            List.of(
                new InternalDateRange.Bucket(
                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                    1.6266528E12,
                    1.6266564E12,
                    1,
                    Collections.singletonList(new InternalAvg("ticketPrice", 841.265625, 1L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                )
            ),
            DocValueFormat.RAW,
            true,
            Collections.emptyMap()
        );
        ex = expectThrows(
            Exception.class,
            () -> metricCorrelationFunction.execute(
                new MlAggsHelper.DoubleBucketValues(new long[] { 1 }, new double[] { 0.4 }),
                new Aggregations(Collections.singletonList(singleBucket))
            )
        );
        assertThat(ex.getMessage(), containsString("to correlate, there must be at least two buckets"));
    }

    public void testExecute() {
        InternalDateRange agg = new InternalDateRange.Factory().create(
            "daily",
            Arrays.asList(
                new InternalDateRange.Bucket(
                    "*-2021-07-19T00:00:00.000Z",
                    Double.NaN,
                    1.6266528E12,
                    10,
                    Collections.singletonList(new InternalAvg("ticketPrice", 0L, 10L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                ),
                new InternalDateRange.Bucket(
                    "2021-07-19T00:00:00.000Z-2021-07-19T01:00:00.000Z",
                    1.6266528E12,
                    1.6266564E12,
                    5,
                    Collections.singletonList(new InternalAvg("ticketPrice", 841.265625, 5L, DocValueFormat.RAW, Collections.emptyMap())),
                    true,
                    DocValueFormat.RAW
                )
            ),
            DocValueFormat.RAW,
            true,
            Collections.emptyMap()
        );

        MetricCorrelationFunction metricCorrelationFunction = new MetricCorrelationFunction("daily>_count");
        assertThat(
            metricCorrelationFunction.execute(
                new MlAggsHelper.DoubleBucketValues(new long[] { 0, 1 }, new double[] { 100, 50 }),
                new Aggregations(Collections.singletonList(agg))
            ),
            greaterThan(0.5)
        );

        assertThat(
            metricCorrelationFunction.execute(
                new MlAggsHelper.DoubleBucketValues(new long[] { 0, 1 }, new double[] { 50, 100 }),
                new Aggregations(Collections.singletonList(agg))
            ),
            lessThan(0.5)
        );
    }
}
