/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.Result;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockCardinality;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilters;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFiltersBucket;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTermsBucket;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.TupleMatchers.isTuple;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
        return new MulticlassConfusionMatrix(size, null);
    }

    public void testConstructor_SizeValidationFailures() {
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(-1, null));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(0, null));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(1001, null));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
    }

    public void testAggs() {
        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix();
        Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs = confusionMatrix.aggs("act", "pred");
        assertThat(aggs, isTuple(not(empty()), empty()));
        assertThat(confusionMatrix.getResult(), isEmpty());
    }

    public void testEvaluate() {
        Aggregations aggs = new Aggregations(List.of(
            mockTerms(
                MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                List.of(
                    mockTermsBucket("dog", new Aggregations(List.of())),
                    mockTermsBucket("cat", new Aggregations(List.of()))),
                0L),
            mockFilters(
                MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                List.of(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(List.of(mockFilters(
                            MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                            List.of(mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        70,
                        new Aggregations(List.of(mockFilters(
                            MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                            List.of(mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 0L)))))))),
            mockCardinality(MulticlassConfusionMatrix.STEP_2_CARDINALITY_OF_ACTUAL_CLASS, 2L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2, null);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), isTuple(empty(), empty()));
        Result result = confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
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
                MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                List.of(
                    mockTermsBucket("dog", new Aggregations(List.of())),
                    mockTermsBucket("cat", new Aggregations(List.of()))),
                100L),
            mockFilters(
                MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                List.of(
                    mockFiltersBucket(
                        "dog",
                        30,
                        new Aggregations(List.of(mockFilters(
                            MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                            List.of(mockFiltersBucket("cat", 10L), mockFiltersBucket("dog", 20L), mockFiltersBucket("_other_", 0L)))))),
                    mockFiltersBucket(
                        "cat",
                        85,
                        new Aggregations(List.of(mockFilters(
                            MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                            List.of(mockFiltersBucket("cat", 30L), mockFiltersBucket("dog", 40L), mockFiltersBucket("_other_", 15L)))))))),
            mockCardinality(MulticlassConfusionMatrix.STEP_2_CARDINALITY_OF_ACTUAL_CLASS, 5L)));

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2, null);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs("act", "pred"), isTuple(empty(), empty()));
        Result result = confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass("dog", 30, List.of(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 85, List.of(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 15))));
        assertThat(result.getOtherActualClassCount(), equalTo(3L));
    }
}
