/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockExtendedStats;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.hamcrest.Matchers.equalTo;

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
        Aggregations aggs = new Aggregations(
            Arrays.asList(
                mockSingleValue("residual_sum_of_squares", 10_111),
                mockExtendedStats("extended_stats_actual", 155.23, 1000),
                mockExtendedStats("some_other_extended_stats", 99.1, 10_000),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            )
        );

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        String expected = "{\"value\":0.9348643947690524}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluateWithZeroCount() {
        Aggregations aggs = new Aggregations(
            Arrays.asList(
                mockSingleValue("residual_sum_of_squares", 0),
                mockExtendedStats("extended_stats_actual", 0.0, 0),
                mockExtendedStats("some_other_extended_stats", 99.1, 10_000),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            )
        );

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }

    public void testEvaluateWithSingleCountZeroVariance() {
        Aggregations aggs = new Aggregations(
            Arrays.asList(
                mockSingleValue("residual_sum_of_squares", 1),
                mockExtendedStats("extended_stats_actual", 0.0, 1),
                mockExtendedStats("some_other_extended_stats", 99.1, 10_000),
                mockSingleValue("some_other_single_metric_agg", 0.2377)
            )
        );

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }

    public void testEvaluate_GivenMissingAggs() {
        Aggregations aggs = new Aggregations(Collections.singletonList(mockSingleValue("some_other_single_metric_agg", 0.2377)));

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }

    public void testEvaluate_GivenMissingExtendedStatsAgg() {
        Aggregations aggs = new Aggregations(
            Arrays.asList(mockSingleValue("some_other_single_metric_agg", 0.2377), mockSingleValue("residual_sum_of_squares", 0.2377))
        );

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }

    public void testEvaluate_GivenMissingResidualSumOfSquaresAgg() {
        Aggregations aggs = new Aggregations(
            Arrays.asList(mockSingleValue("some_other_single_metric_agg", 0.2377), mockExtendedStats("extended_stats_actual", 100, 50))
        );

        RSquared rSquared = new RSquared();
        rSquared.process(aggs);

        EvaluationMetricResult result = rSquared.getResult().get();
        assertThat(result, equalTo(new RSquared.Result(0.0)));
    }
}
