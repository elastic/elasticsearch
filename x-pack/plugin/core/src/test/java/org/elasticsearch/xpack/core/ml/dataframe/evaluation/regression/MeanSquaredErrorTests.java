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
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeanSquaredErrorTests extends AbstractSerializingTestCase<MeanSquaredError> {

    @Override
    protected MeanSquaredError doParseInstance(XContentParser parser) throws IOException {
        return MeanSquaredError.fromXContent(parser);
    }

    @Override
    protected MeanSquaredError createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<MeanSquaredError> instanceReader() {
        return MeanSquaredError::new;
    }

    public static MeanSquaredError createRandom() {
        return new MeanSquaredError();
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            createSingleMetricAgg("regression_mean_squared_error", 0.8123),
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        MeanSquaredError mse = new MeanSquaredError();
        EvaluationMetricResult result = mse.evaluate(aggs);

        String expected = "{\"error\":0.8123}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenMissingAggs() {
        Aggregations aggs = new Aggregations(Collections.singletonList(
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        MeanSquaredError mse = new MeanSquaredError();
        EvaluationMetricResult result = mse.evaluate(aggs);
        assertThat(result, equalTo(new MeanSquaredError.Result(0.0)));
    }

    private static NumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }
}
