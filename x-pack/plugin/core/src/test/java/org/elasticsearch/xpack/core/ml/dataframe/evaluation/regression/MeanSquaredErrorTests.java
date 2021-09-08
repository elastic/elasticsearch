/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.hamcrest.Matchers.equalTo;

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
            mockSingleValue("regression_mse", 0.8123),
            mockSingleValue("some_other_single_metric_agg", 0.2377)
        ));

        MeanSquaredError mse = new MeanSquaredError();
        mse.process(aggs);

        EvaluationMetricResult result = mse.getResult().get();
        String expected = "{\"value\":0.8123}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenMissingAggs() {
        Aggregations aggs = new Aggregations(Collections.singletonList(
            mockSingleValue("some_other_single_metric_agg", 0.2377)
        ));

        MeanSquaredError mse = new MeanSquaredError();
        mse.process(aggs);

        EvaluationMetricResult result = mse.getResult().get();
        assertThat(result, equalTo(new MeanSquaredError.Result(0.0)));
    }
}
