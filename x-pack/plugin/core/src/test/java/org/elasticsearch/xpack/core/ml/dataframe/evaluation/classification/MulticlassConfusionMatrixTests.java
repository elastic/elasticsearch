/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MulticlassConfusionMatrixTests extends AbstractSerializingTestCase<MulticlassConfusionMatrix> {

    @Override
    protected MulticlassConfusionMatrix doParseInstance(XContentParser parser) throws IOException {
        return MulticlassConfusionMatrix.fromXContent(parser);
    }

    @Override
    protected MulticlassConfusionMatrix createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<MulticlassConfusionMatrix> instanceReader() {
        return MulticlassConfusionMatrix::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static MulticlassConfusionMatrix createRandom() {
        Integer size = randomBoolean() ? null : randomIntBetween(1, 1000);
        return new MulticlassConfusionMatrix(size);
    }

    public void testConstructor_SizeValidationFailures() {
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(-1));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(0));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(1001));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
    }

    public void testAggs() {
        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix();
        List<AggregationBuilder> aggs = confusionMatrix.aggs("act", "pred");
        assertThat(aggs, is(not(empty())));
        assertThat(confusionMatrix.getResult(), equalTo(Optional.empty()));
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(List.of(
            mockTerms(
                "multiclass_confusion_matrix_step_1_by_actual_class",
                List.of(
                    mockTermsBucket("dog", new Aggregations(List.of())),
                    mockTermsBucket("cat", new Aggregations(List.of()))),
                0L),
            mockFilters(
                "multiclass_confusion_matrix_step_2_by_actual_class",
                List.of(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(List.of(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            List.of(mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        70,
                        new Aggregations(List.of(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            List.of(mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 0L)))))))),
            mockCardinality("multiclass_confusion_matrix_step_2_cardinality_of_actual_class", 2L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), is(empty()));
        MulticlassConfusionMatrix.Result result = (MulticlassConfusionMatrix.Result) confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo("multiclass_confusion_matrix"));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass("dog", 30, List.of(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 70, List.of(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 0))));
        assertThat(result.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_OtherClassesCountGreaterThanZero() {
        Aggregations aggs = new Aggregations(List.of(
            mockTerms(
                "multiclass_confusion_matrix_step_1_by_actual_class",
                List.of(
                    mockTermsBucket("dog", new Aggregations(List.of())),
                    mockTermsBucket("cat", new Aggregations(List.of()))),
                100L),
            mockFilters(
                "multiclass_confusion_matrix_step_2_by_actual_class",
                List.of(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(List.of(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            List.of(mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        85,
                        new Aggregations(List.of(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            List.of(mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 15L)))))))),
            mockCardinality("multiclass_confusion_matrix_step_2_cardinality_of_actual_class", 5L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), is(empty()));
        MulticlassConfusionMatrix.Result result = (MulticlassConfusionMatrix.Result) confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo("multiclass_confusion_matrix"));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass("dog", 30, List.of(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 85, List.of(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 15))));
        assertThat(result.getOtherActualClassCount(), equalTo(3L));
    }

    private static Terms mockTerms(String name, List<Terms.Bucket> buckets, long sumOfOtherDocCounts) {
        Terms aggregation = mock(Terms.class);
        when(aggregation.getName()).thenReturn(name);
        doReturn(buckets).when(aggregation).getBuckets();
        when(aggregation.getSumOfOtherDocCounts()).thenReturn(sumOfOtherDocCounts);
        return aggregation;
    }

    private static Terms.Bucket mockTermsBucket(String key, Aggregations subAggs) {
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    private static Filters mockFilters(String name, List<Filters.Bucket> buckets) {
        Filters aggregation = mock(Filters.class);
        when(aggregation.getName()).thenReturn(name);
        doReturn(buckets).when(aggregation).getBuckets();
        return aggregation;
    }

    private static Filters.Bucket mockFiltersBucket(String key, long docCount, Aggregations subAggs) {
        Filters.Bucket bucket = mockFiltersBucket(key, docCount);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    private static Filters.Bucket mockFiltersBucket(String key, long docCount) {
        Filters.Bucket bucket = mock(Filters.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getDocCount()).thenReturn(docCount);
        return bucket;
    }

    private static Cardinality mockCardinality(String name, long value) {
        Cardinality aggregation = mock(Cardinality.class);
        when(aggregation.getName()).thenReturn(name);
        when(aggregation.getValue()).thenReturn(value);
        return aggregation;
    }
}
