/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.Result;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.TupleMatchers.isTuple;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockCardinality;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilters;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFiltersBucket;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTermsBucket;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MulticlassConfusionMatrixTests extends AbstractXContentSerializingTestCase<MulticlassConfusionMatrix> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);
    private static final EvaluationFields EVALUATION_FIELDS = new EvaluationFields("foo", "bar", null, null, null, true);

    @Override
    protected MulticlassConfusionMatrix doParseInstance(XContentParser parser) throws IOException {
        return MulticlassConfusionMatrix.fromXContent(parser);
    }

    @Override
    protected MulticlassConfusionMatrix createTestInstance() {
        return createRandom();
    }

    @Override
    protected MulticlassConfusionMatrix mutateInstance(MulticlassConfusionMatrix instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> new MulticlassConfusionMatrix(-1, null)
            );
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new MulticlassConfusionMatrix(0, null));
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
        {
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> new MulticlassConfusionMatrix(1001, null)
            );
            assertThat(e.getMessage(), equalTo("[size] must be an integer in [1, 1000]"));
        }
    }

    public void testAggs() {
        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix();
        Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs = confusionMatrix.aggs(
            EVALUATION_PARAMETERS,
            EVALUATION_FIELDS
        );
        assertThat(aggs, isTuple(not(empty()), empty()));
        assertThat(confusionMatrix.getResult(), isEmpty());
    }

    public void testProcess() {
        InternalAggregations aggs = InternalAggregations.from(
            List.of(
                mockTerms(
                    MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockTermsBucket("dog", InternalAggregations.from(List.of())),
                        mockTermsBucket("cat", InternalAggregations.from(List.of()))
                    ),
                    0L
                ),
                mockCardinality(MulticlassConfusionMatrix.STEP_1_CARDINALITY_OF_ACTUAL_CLASS, 2L),
                mockFilters(
                    MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "dog",
                            30,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("cat", 10L),
                                            mockFiltersBucket("dog", 20L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        ),
                        mockFiltersBucket(
                            "cat",
                            70,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("cat", 30L),
                                            mockFiltersBucket("dog", 40L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2, null);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS), isTuple(empty(), empty()));
        Result result = confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass("dog", 30, List.of(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 70, List.of(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 0)
                )
            )
        );
        assertThat(result.getOtherActualClassCount(), equalTo(0L));
    }

    public void testProcess_OtherClassesCountGreaterThanZero() {
        InternalAggregations aggs = InternalAggregations.from(
            List.of(
                mockTerms(
                    MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockTermsBucket("dog", InternalAggregations.from(List.of())),
                        mockTermsBucket("cat", InternalAggregations.from(List.of()))
                    ),
                    100L
                ),
                mockCardinality(MulticlassConfusionMatrix.STEP_1_CARDINALITY_OF_ACTUAL_CLASS, 5L),
                mockFilters(
                    MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "dog",
                            30,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("cat", 10L),
                                            mockFiltersBucket("dog", 20L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        ),
                        mockFiltersBucket(
                            "cat",
                            85,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("cat", 30L),
                                            mockFiltersBucket("dog", 40L),
                                            mockFiltersBucket("_other_", 15L)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(2, null);
        confusionMatrix.process(aggs);

        assertThat(confusionMatrix.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS), isTuple(empty(), empty()));
        Result result = confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass("dog", 30, List.of(new PredictedClass("cat", 10L), new PredictedClass("dog", 20L)), 0),
                    new ActualClass("cat", 85, List.of(new PredictedClass("cat", 30L), new PredictedClass("dog", 40L)), 15)
                )
            )
        );
        assertThat(result.getOtherActualClassCount(), equalTo(3L));
    }

    public void testProcess_MoreThanTwoStepsNeeded() {
        InternalAggregations aggsStep1 = InternalAggregations.from(
            List.of(
                mockTerms(
                    MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockTermsBucket("ant", InternalAggregations.from(List.of())),
                        mockTermsBucket("cat", InternalAggregations.from(List.of())),
                        mockTermsBucket("dog", InternalAggregations.from(List.of())),
                        mockTermsBucket("fox", InternalAggregations.from(List.of()))
                    ),
                    0L
                ),
                mockCardinality(MulticlassConfusionMatrix.STEP_1_CARDINALITY_OF_ACTUAL_CLASS, 2L)
            )
        );
        InternalAggregations aggsStep2 = InternalAggregations.from(
            List.of(
                mockFilters(
                    MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "ant",
                            46,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("ant", 10L),
                                            mockFiltersBucket("cat", 11L),
                                            mockFiltersBucket("dog", 12L),
                                            mockFiltersBucket("fox", 13L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        ),
                        mockFiltersBucket(
                            "cat",
                            86,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("ant", 20L),
                                            mockFiltersBucket("cat", 21L),
                                            mockFiltersBucket("dog", 22L),
                                            mockFiltersBucket("fox", 23L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );
        InternalAggregations aggsStep3 = InternalAggregations.from(
            List.of(
                mockFilters(
                    MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "dog",
                            126,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("ant", 30L),
                                            mockFiltersBucket("cat", 31L),
                                            mockFiltersBucket("dog", 32L),
                                            mockFiltersBucket("fox", 33L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        ),
                        mockFiltersBucket(
                            "fox",
                            166,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
                                        List.of(
                                            mockFiltersBucket("ant", 40L),
                                            mockFiltersBucket("cat", 41L),
                                            mockFiltersBucket("dog", 42L),
                                            mockFiltersBucket("fox", 43L),
                                            mockFiltersBucket("_other_", 0L)
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );

        MulticlassConfusionMatrix confusionMatrix = new MulticlassConfusionMatrix(4, null);
        confusionMatrix.process(aggsStep1);
        confusionMatrix.process(aggsStep2);
        confusionMatrix.process(aggsStep3);

        assertThat(confusionMatrix.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS), isTuple(empty(), empty()));
        Result result = confusionMatrix.getResult().get();
        assertThat(result.getMetricName(), equalTo(MulticlassConfusionMatrix.NAME.getPreferredName()));
        assertThat(
            result.getConfusionMatrix(),
            equalTo(
                List.of(
                    new ActualClass(
                        "ant",
                        46,
                        List.of(
                            new PredictedClass("ant", 10L),
                            new PredictedClass("cat", 11L),
                            new PredictedClass("dog", 12L),
                            new PredictedClass("fox", 13L)
                        ),
                        0
                    ),
                    new ActualClass(
                        "cat",
                        86,
                        List.of(
                            new PredictedClass("ant", 20L),
                            new PredictedClass("cat", 21L),
                            new PredictedClass("dog", 22L),
                            new PredictedClass("fox", 23L)
                        ),
                        0
                    ),
                    new ActualClass(
                        "dog",
                        126,
                        List.of(
                            new PredictedClass("ant", 30L),
                            new PredictedClass("cat", 31L),
                            new PredictedClass("dog", 32L),
                            new PredictedClass("fox", 33L)
                        ),
                        0
                    ),
                    new ActualClass(
                        "fox",
                        166,
                        List.of(
                            new PredictedClass("ant", 40L),
                            new PredictedClass("cat", 41L),
                            new PredictedClass("dog", 42L),
                            new PredictedClass("fox", 43L)
                        ),
                        0
                    )
                )
            )
        );
        assertThat(result.getOtherActualClassCount(), equalTo(0L));
    }
}
