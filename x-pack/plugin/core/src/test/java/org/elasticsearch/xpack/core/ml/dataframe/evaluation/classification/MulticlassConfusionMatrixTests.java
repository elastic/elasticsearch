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
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockCardinality;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilters;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFiltersBucket;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTermsBucket;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
        assertThat(confusionMatrix.getResult(), isEmpty());
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(
                "multiclass_confusion_matrix_step_1_by_actual_class",
                Arrays.asList(
                    mockTermsBucket("dog", new Aggregations(Collections.emptyList())),
                    mockTermsBucket("cat", new Aggregations(Collections.emptyList()))),
                0L),
            mockFilters(
                "multiclass_confusion_matrix_step_2_by_actual_class",
                Arrays.asList(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(Arrays.asList(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            Arrays.asList(
                                mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        70,
                        new Aggregations(Arrays.asList(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            Arrays.asList(
                                mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 0L)))))))),
            mockCardinality("multiclass_confusion_matrix_step_2_cardinality_of_actual_class", 2L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), is(empty()));
        MulticlassConfusionMatrix.Result result = (MulticlassConfusionMatrix.Result) confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo("multiclass_confusion_matrix"));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                Arrays.asList(
                    new ActualClass("dog", 30, Arrays.asList(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 70, Arrays.asList(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 0))));
        assertThat(result.getOtherActualClassCount(), equalTo(0L));
    }

    public void testEvaluate_OtherClassesCountGreaterThanZero() {
        Aggregations aggs = new Aggregations(Arrays.asList(
            mockTerms(
                "multiclass_confusion_matrix_step_1_by_actual_class",
                Arrays.asList(
                    mockTermsBucket("dog", new Aggregations(Collections.emptyList())),
                    mockTermsBucket("cat", new Aggregations(Collections.emptyList()))),
                100L),
            mockFilters(
                "multiclass_confusion_matrix_step_2_by_actual_class",
                Arrays.asList(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(Arrays.asList(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            Arrays.asList(
                                mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        85,
                        new Aggregations(Arrays.asList(mockFilters(
                            "multiclass_confusion_matrix_step_2_by_predicted_class",
                            Arrays.asList(
                                mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 15L)))))))),
            mockCardinality("multiclass_confusion_matrix_step_2_cardinality_of_actual_class", 5L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), is(empty()));
        MulticlassConfusionMatrix.Result result = (MulticlassConfusionMatrix.Result) confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo("multiclass_confusion_matrix"));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                Arrays.asList(
                    new ActualClass("dog", 30, Arrays.asList(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 85, Arrays.asList(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 15))));
        assertThat(result.getOtherActualClassCount(), equalTo(3L));
    }
}
