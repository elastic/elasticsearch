/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilter;
import static org.hamcrest.Matchers.equalTo;

public class RecallTests extends AbstractXContentSerializingTestCase<Recall> {

    @Override
    protected Recall doParseInstance(XContentParser parser) throws IOException {
        return Recall.fromXContent(parser);
    }

    @Override
    protected Recall createTestInstance() {
        return createRandom();
    }

    @Override
    protected Recall mutateInstance(Recall instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Recall> instanceReader() {
        return Recall::new;
    }

    public static Recall createRandom() {
        int thresholdsSize = randomIntBetween(1, 3);
        List<Double> thresholds = new ArrayList<>(thresholdsSize);
        for (int i = 0; i < thresholdsSize; i++) {
            thresholds.add(randomDouble());
        }
        return new Recall(thresholds);
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(
            Arrays.asList(
                mockFilter("recall_at_0.25_TP", 1L),
                mockFilter("recall_at_0.25_FN", 4L),
                mockFilter("recall_at_0.5_TP", 3L),
                mockFilter("recall_at_0.5_FN", 1L),
                mockFilter("recall_at_0.75_TP", 5L),
                mockFilter("recall_at_0.75_FN", 0L)
            )
        );

        Recall recall = new Recall(Arrays.asList(0.25, 0.5, 0.75));
        EvaluationMetricResult result = recall.evaluate(aggs);

        String expected = """
            {"0.25":0.2,"0.5":0.75,"0.75":1.0}""";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenZeroTpAndFp() {
        Aggregations aggs = new Aggregations(Arrays.asList(mockFilter("recall_at_1.0_TP", 0L), mockFilter("recall_at_1.0_FN", 0L)));

        Recall recall = new Recall(Arrays.asList(1.0));
        EvaluationMetricResult result = recall.evaluate(aggs);

        String expected = "{\"1.0\":0.0}";
        assertThat(Strings.toString(result), equalTo(expected));
    }
}
