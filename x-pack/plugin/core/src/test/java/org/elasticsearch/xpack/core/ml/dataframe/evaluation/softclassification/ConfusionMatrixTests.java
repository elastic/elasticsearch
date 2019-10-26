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
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        SoftClassificationMetric.ClassInfo classInfo = mock(SoftClassificationMetric.ClassInfo.class);
        when(classInfo.getName()).thenReturn("foo");

        Aggregations aggs = new Aggregations(Arrays.asList(
            createFilterAgg("confusion_matrix_foo_at_0.25_TP", 1L),
            createFilterAgg("confusion_matrix_foo_at_0.25_FP", 2L),
            createFilterAgg("confusion_matrix_foo_at_0.25_TN", 3L),
            createFilterAgg("confusion_matrix_foo_at_0.25_FN", 4L),
            createFilterAgg("confusion_matrix_foo_at_0.5_TP", 5L),
            createFilterAgg("confusion_matrix_foo_at_0.5_FP", 6L),
            createFilterAgg("confusion_matrix_foo_at_0.5_TN", 7L),
            createFilterAgg("confusion_matrix_foo_at_0.5_FN", 8L)
        ));

        ConfusionMatrix confusionMatrix = new ConfusionMatrix(Arrays.asList(0.25, 0.5));
        EvaluationMetricResult result = confusionMatrix.evaluate(classInfo, aggs);

        String expected = "{\"0.25\":{\"tp\":1,\"fp\":2,\"tn\":3,\"fn\":4},\"0.5\":{\"tp\":5,\"fp\":6,\"tn\":7,\"fn\":8}}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    private static Filter createFilterAgg(String name, long docCount) {
        Filter agg = mock(Filter.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getDocCount()).thenReturn(docCount);
        return agg;
    }
}
