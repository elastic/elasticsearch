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

public class PrecisionTests extends AbstractSerializingTestCase<Precision> {

    @Override
    protected Precision doParseInstance(XContentParser parser) throws IOException {
        return Precision.fromXContent(parser);
    }

    @Override
    protected Precision createTestInstance() {
        return createRandom();
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
        SoftClassificationMetric.ClassInfo classInfo = mock(SoftClassificationMetric.ClassInfo.class);
        when(classInfo.getName()).thenReturn("foo");

        Aggregations aggs = new Aggregations(Arrays.asList(
            createFilterAgg("precision_foo_at_0.25_TP", 1L),
            createFilterAgg("precision_foo_at_0.25_FP", 4L),
            createFilterAgg("precision_foo_at_0.5_TP", 3L),
            createFilterAgg("precision_foo_at_0.5_FP", 1L),
            createFilterAgg("precision_foo_at_0.75_TP", 5L),
            createFilterAgg("precision_foo_at_0.75_FP", 0L)
        ));

        Precision precision = new Precision(Arrays.asList(0.25, 0.5, 0.75));
        EvaluationMetricResult result = precision.evaluate(classInfo, aggs);

        String expected = "{\"0.25\":0.2,\"0.5\":0.75,\"0.75\":1.0}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenZeroTpAndFp() {
        SoftClassificationMetric.ClassInfo classInfo = mock(SoftClassificationMetric.ClassInfo.class);
        when(classInfo.getName()).thenReturn("foo");

        Aggregations aggs = new Aggregations(Arrays.asList(
            createFilterAgg("precision_foo_at_1.0_TP", 0L),
            createFilterAgg("precision_foo_at_1.0_FP", 0L)
        ));

        Precision precision = new Precision(Arrays.asList(1.0));
        EvaluationMetricResult result = precision.evaluate(classInfo, aggs);

        String expected = "{\"1.0\":0.0}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    private static Filter createFilterAgg(String name, long docCount) {
        Filter agg = mock(Filter.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getDocCount()).thenReturn(docCount);
        return agg;
    }
}
