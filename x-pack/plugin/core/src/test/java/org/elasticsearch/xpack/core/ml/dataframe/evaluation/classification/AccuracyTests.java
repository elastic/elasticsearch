/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccuracyTests extends AbstractSerializingTestCase<Accuracy> {

    @Override
    protected Accuracy doParseInstance(XContentParser parser) throws IOException {
        return Accuracy.fromXContent(parser);
    }

    @Override
    protected Accuracy createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Accuracy> instanceReader() {
        return Accuracy::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static Accuracy createRandom() {
        return new Accuracy();
    }

    public void testProcess() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            createTermsAgg("classification_classes"),
            createSingleMetricAgg("classification_overall_accuracy", 0.8123),
            createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
        ));

        Accuracy accuracy = new Accuracy();
        accuracy.process(aggs);

        assertThat(accuracy.getResult().get(), equalTo(new Accuracy.Result(Collections.emptyList(), 0.8123)));
    }

    public void testProcess_GivenMissingAgg() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                createTermsAgg("classification_classes"),
                createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
            ));
            Accuracy accuracy = new Accuracy();
            expectThrows(NullPointerException.class, () -> accuracy.process(aggs));
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                createSingleMetricAgg("classification_overall_accuracy", 0.8123),
                createSingleMetricAgg("some_other_single_metric_agg", 0.2377)
            ));
            Accuracy accuracy = new Accuracy();
            expectThrows(NullPointerException.class, () -> accuracy.process(aggs));
        }
    }

    public void testProcess_GivenAggOfWrongType() {
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                createTermsAgg("classification_classes"),
                createTermsAgg("classification_overall_accuracy")
            ));
            Accuracy accuracy = new Accuracy();
            expectThrows(ClassCastException.class, () -> accuracy.process(aggs));
        }
        {
            Aggregations aggs = new Aggregations(Arrays.asList(
                createSingleMetricAgg("classification_classes", 1.0),
                createSingleMetricAgg("classification_overall_accuracy", 0.8123)
            ));
            Accuracy accuracy = new Accuracy();
            expectThrows(ClassCastException.class, () -> accuracy.process(aggs));
        }
    }

    private static NumericMetricsAggregation.SingleValue createSingleMetricAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    private static Terms createTermsAgg(String name) {
        Terms agg = mock(Terms.class);
        when(agg.getName()).thenReturn(name);
        return agg;
    }
}
