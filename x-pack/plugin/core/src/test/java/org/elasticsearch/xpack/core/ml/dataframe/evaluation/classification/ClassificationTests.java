/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClassificationTests extends AbstractSerializingTestCase<Classification> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(MlEvaluationNamedXContentProvider.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static Classification createRandom() {
        List<EvaluationMetric> metrics =
            randomSubsetOf(
                Arrays.asList(
                    AccuracyTests.createRandom(),
                    PrecisionTests.createRandom(),
                    RecallTests.createRandom(),
                    MulticlassConfusionMatrixTests.createRandom()));
        return new Classification(randomAlphaOfLength(10), randomAlphaOfLength(10), metrics.isEmpty() ? null : metrics);
    }

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser);
    }

    @Override
    protected Classification createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Classification> instanceReader() {
        return Classification::new;
    }

    public void testConstructor_GivenEmptyMetrics() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", "bar", Collections.emptyList()));
        assertThat(e.getMessage(), equalTo("[classification] must have one or more metrics"));
    }

    public void testBuildSearch() {
        QueryBuilder userProvidedQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field_A", "some-value"))
                .filter(QueryBuilders.termQuery("field_B", "some-other-value"));
        QueryBuilder expectedSearchQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery("act"))
                .filter(QueryBuilders.existsQuery("pred"))
                .filter(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("field_A", "some-value"))
                    .filter(QueryBuilders.termQuery("field_B", "some-other-value")));

        Classification evaluation = new Classification("act", "pred", Arrays.asList(new MulticlassConfusionMatrix()));

        SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(EVALUATION_PARAMETERS, userProvidedQuery);
        assertThat(searchSourceBuilder.query(), equalTo(expectedSearchQuery));
        assertThat(searchSourceBuilder.aggregations().count(), greaterThan(0));
    }

    public void testProcess_MultipleMetricsWithDifferentNumberOfSteps() {
        EvaluationMetric metric1 = new FakeClassificationMetric("fake_metric_1", 2);
        EvaluationMetric metric2 = new FakeClassificationMetric("fake_metric_2", 3);
        EvaluationMetric metric3 = new FakeClassificationMetric("fake_metric_3", 4);
        EvaluationMetric metric4 = new FakeClassificationMetric("fake_metric_4", 5);

        Classification evaluation = new Classification("act", "pred", Arrays.asList(metric1, metric2, metric3, metric4));
        assertThat(metric1.getResult(), isEmpty());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isEmpty());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isPresent());
        assertThat(evaluation.hasAllResults(), is(true));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isPresent());
        assertThat(evaluation.hasAllResults(), is(true));
    }

    private static SearchResponse mockSearchResponseWithNonZeroTotalHits() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits hits = new SearchHits(SearchHits.EMPTY, new TotalHits(10, TotalHits.Relation.EQUAL_TO), 0);
        when(searchResponse.getHits()).thenReturn(hits);
        return searchResponse;
    }

    /**
     * Metric which iterates through its steps in {@link #process} method.
     * Number of steps is configurable.
     * Upon reaching the last step, the result is produced.
     */
    private static class FakeClassificationMetric implements EvaluationMetric {

        private final String name;
        private final int numSteps;
        private int currentStepIndex;
        private EvaluationMetricResult result;

        FakeClassificationMetric(String name, int numSteps) {
            this.name = name;
            this.numSteps = numSteps;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                      String actualField,
                                                                                      String predictedField) {
            return Tuple.tuple(List.of(), List.of());
        }

        @Override
        public void process(Aggregations aggs) {
            if (result != null) {
                return;
            }
            currentStepIndex++;
            if (currentStepIndex == numSteps) {
                // This is the last step, time to write evaluation result
                result = mock(EvaluationMetricResult.class);
            }
        }

        @Override
        public Optional<EvaluationMetricResult> getResult() {
            return Optional.ofNullable(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) {
        }
    }
}
