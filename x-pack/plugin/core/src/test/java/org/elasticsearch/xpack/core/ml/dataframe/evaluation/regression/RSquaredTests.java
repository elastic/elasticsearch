/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RSquaredTests extends AbstractSerializingTestCase<RSquared> {

    @Override
    protected RSquared doParseInstance(XContentParser parser) throws IOException {
        return RSquared.fromXContent(parser);
    }

    @Override
    protected RSquared createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<RSquared> instanceReader() {
        return RSquared::new;
    }

    public static RSquared createRandom() {
        return new RSquared();
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            createSingleMetricAgg("residual_sum_of_squares", 10_111),
            createExtendedStatsAgg("extended_stats_actual", 155.23, 1000),
            createExtendedStatsAgg("some_other_extended_stats",99.1, 10_000),
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        RSquared rSquared = new RSquared();
        EvaluationMetricResult result = rSquared.evaluate(aggs);

        String expected = "{\"value\":0.9348643947690524}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluateWithZeroCount() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            createSingleMetricAgg("residual_sum_of_squares", 0),
            createExtendedStatsAgg("extended_stats_actual", 0.0, 0),
            createExtendedStatsAgg("some_other_extended_stats",99.1, 10_000),
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        RSquared rSquared = new RSquared();
        EvaluationMetricResult result = rSquared.evaluate(aggs);
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }

    public void testEvaluate_GivenMissingAggs() {
        EvaluationMetricResult zeroResult = new RSquared.Result(0.0);
        Aggregations aggs = new Aggregations(Collections.singletonList(
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        RSquared rSquared = new RSquared();
        EvaluationMetricResult result = rSquared.evaluate(aggs);
        assertThat(result, equalTo(zeroResult));

        aggs = new Aggregations(Arrays.asList(
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377),
            createSingleMetricAgg("residual_sum_of_squares", 0.2377)
        ));

        result = rSquared.evaluate(aggs);
        assertThat(result, equalTo(zeroResult));

        aggs = new Aggregations(Arrays.asList(
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377),
            createExtendedStatsAgg("extended_stats_actual",100, 50)
        ));

        result = rSquared.evaluate(aggs);
        assertThat(result, equalTo(zeroResult));
    }

    private static NumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    private static ExtendedStats createExtendedStatsAgg(String name, double variance, long count) {
        ExtendedStats agg = mock(ExtendedStats.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getVariance()).thenReturn(variance);
        when(agg.getCount()).thenReturn(count);
        return agg;
    }
}
