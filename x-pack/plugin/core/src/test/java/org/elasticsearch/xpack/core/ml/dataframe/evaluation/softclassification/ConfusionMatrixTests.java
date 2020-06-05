/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilter;
import static org.hamcrest.Matchers.equalTo;

public class ConfusionMatrixTests extends AbstractSerializingTestCase<ConfusionMatrix> {

    @Override
    protected ConfusionMatrix doParseInstance(XContentParser parser) throws IOException {
        return ConfusionMatrix.fromXContent(parser);
    }

    @Override
    protected ConfusionMatrix createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<ConfusionMatrix> instanceReader() {
        return ConfusionMatrix::new;
    }

    public static ConfusionMatrix createRandom() {
        int thresholdsSize = randomIntBetween(1, 3);
        List<Double> thresholds = new ArrayList<>(thresholdsSize);
        for (int i = 0; i < thresholdsSize; i++) {
            thresholds.add(randomDouble());
        }
        return new ConfusionMatrix(thresholds);
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockFilter("confusion_matrix_at_0.25_TP", 1L),
            mockFilter("confusion_matrix_at_0.25_FP", 2L),
            mockFilter("confusion_matrix_at_0.25_TN", 3L),
            mockFilter("confusion_matrix_at_0.25_FN", 4L),
            mockFilter("confusion_matrix_at_0.5_TP", 5L),
            mockFilter("confusion_matrix_at_0.5_FP", 6L),
            mockFilter("confusion_matrix_at_0.5_TN", 7L),
            mockFilter("confusion_matrix_at_0.5_FN", 8L)
        ));

        ConfusionMatrix confusionMatrix = new ConfusionMatrix(Arrays.asList(0.25, 0.5));
        EvaluationMetricResult result = confusionMatrix.evaluate(aggs);

        String expected = "{\"0.25\":{\"tp\":1,\"fp\":2,\"tn\":3,\"fn\":4},\"0.5\":{\"tp\":5,\"fp\":6,\"tn\":7,\"fn\":8}}";
        assertThat(Strings.toString(result), equalTo(expected));
    }
}
