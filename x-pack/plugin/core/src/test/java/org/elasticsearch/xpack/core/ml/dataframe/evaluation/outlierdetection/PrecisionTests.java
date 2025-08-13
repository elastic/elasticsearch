/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilter;
import static org.hamcrest.Matchers.equalTo;

public class PrecisionTests extends AbstractXContentSerializingTestCase<Precision> {

    @Override
    protected Precision doParseInstance(XContentParser parser) throws IOException {
        return Precision.fromXContent(parser);
    }

    @Override
    protected Precision createTestInstance() {
        return createRandom();
    }

    @Override
    protected Precision mutateInstance(Precision instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Precision> instanceReader() {
        return Precision::new;
    }

    public static Precision createRandom() {
        int thresholdsSize = randomIntBetween(1, 3);
        List<Double> thresholds = new ArrayList<>(thresholdsSize);
        for (int i = 0; i < thresholdsSize; i++) {
            thresholds.add(randomDouble());
        }
        return new Precision(thresholds);
    }

    public void testEvaluate() {
        InternalAggregations aggs = InternalAggregations.from(
            Arrays.asList(
                mockFilter("precision_at_0.25_TP", 1L),
                mockFilter("precision_at_0.25_FP", 4L),
                mockFilter("precision_at_0.5_TP", 3L),
                mockFilter("precision_at_0.5_FP", 1L),
                mockFilter("precision_at_0.75_TP", 5L),
                mockFilter("precision_at_0.75_FP", 0L)
            )
        );

        Precision precision = new Precision(Arrays.asList(0.25, 0.5, 0.75));
        EvaluationMetricResult result = precision.evaluate(aggs);

        String expected = """
            {"0.25":0.2,"0.5":0.75,"0.75":1.0}""";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenZeroTpAndFp() {
        InternalAggregations aggs = InternalAggregations.from(
            Arrays.asList(mockFilter("precision_at_1.0_TP", 0L), mockFilter("precision_at_1.0_FP", 0L))
        );

        Precision precision = new Precision(Arrays.asList(1.0));
        EvaluationMetricResult result = precision.evaluate(aggs);

        String expected = "{\"1.0\":0.0}";
        assertThat(Strings.toString(result), equalTo(expected));
    }
}
