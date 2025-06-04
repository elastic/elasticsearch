/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Accuracy.Result;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.TupleMatchers.isTuple;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockCardinality;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFilters;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockFiltersBucket;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTerms;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockTermsBucket;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class AccuracyTests extends AbstractXContentSerializingTestCase<Accuracy> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);
    private static final EvaluationFields EVALUATION_FIELDS = new EvaluationFields("foo", "bar", null, null, null, true);

    @Override
    protected Accuracy doParseInstance(XContentParser parser) throws IOException {
        return Accuracy.fromXContent(parser);
    }

    @Override
    protected Accuracy createTestInstance() {
        return createRandom();
    }

    @Override
    protected Accuracy mutateInstance(Accuracy instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
        InternalAggregations aggs = InternalAggregations.from(
            List.of(
                mockTerms(
                    "accuracy_" + MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockTermsBucket("dog", InternalAggregations.from(List.of())),
                        mockTermsBucket("cat", InternalAggregations.from(List.of()))
                    ),
                    100L
                ),
                mockCardinality("accuracy_" + MulticlassConfusionMatrix.STEP_1_CARDINALITY_OF_ACTUAL_CLASS, 1000L),
                mockFilters(
                    "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "dog",
                            30,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
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
                                        "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
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
                ),
                mockSingleValue(Accuracy.OVERALL_ACCURACY_AGG_NAME, 0.5)
            )
        );

        Accuracy accuracy = new Accuracy();
        accuracy.process(aggs);

        assertThat(accuracy.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS), isTuple(empty(), empty()));

        Result result = accuracy.getResult().get();
        assertThat(result.getMetricName(), equalTo(Accuracy.NAME.getPreferredName()));
        assertThat(result.getClasses(), equalTo(List.of(new PerClassSingleValue("dog", 0.5), new PerClassSingleValue("cat", 0.5))));
        assertThat(result.getOverallAccuracy(), equalTo(0.5));
    }

    public void testProcess_GivenCardinalityTooHigh() {
        InternalAggregations aggs = InternalAggregations.from(
            List.of(
                mockTerms(
                    "accuracy_" + MulticlassConfusionMatrix.STEP_1_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockTermsBucket("dog", InternalAggregations.from(List.of())),
                        mockTermsBucket("cat", InternalAggregations.from(List.of()))
                    ),
                    100L
                ),
                mockCardinality("accuracy_" + MulticlassConfusionMatrix.STEP_1_CARDINALITY_OF_ACTUAL_CLASS, 1001L),
                mockFilters(
                    "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_ACTUAL_CLASS,
                    List.of(
                        mockFiltersBucket(
                            "dog",
                            30,
                            InternalAggregations.from(
                                List.of(
                                    mockFilters(
                                        "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
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
                                        "accuracy_" + MulticlassConfusionMatrix.STEP_2_AGGREGATE_BY_PREDICTED_CLASS,
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
                ),
                mockSingleValue(Accuracy.OVERALL_ACCURACY_AGG_NAME, 0.5)
            )
        );

        Accuracy accuracy = new Accuracy();
        accuracy.aggs(EVALUATION_PARAMETERS, EVALUATION_FIELDS);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> accuracy.process(aggs));
        assertThat(e.getMessage(), containsString("Cardinality of field [foo] is too high"));
    }

    public void testComputePerClassAccuracy() {
        assertThat(
            Accuracy.computePerClassAccuracy(
                new MulticlassConfusionMatrix.Result(
                    List.of(
                        new MulticlassConfusionMatrix.ActualClass(
                            "A",
                            14,
                            List.of(
                                new MulticlassConfusionMatrix.PredictedClass("A", 1),
                                new MulticlassConfusionMatrix.PredictedClass("B", 6),
                                new MulticlassConfusionMatrix.PredictedClass("C", 4)
                            ),
                            3L
                        ),
                        new MulticlassConfusionMatrix.ActualClass(
                            "B",
                            20,
                            List.of(
                                new MulticlassConfusionMatrix.PredictedClass("A", 5),
                                new MulticlassConfusionMatrix.PredictedClass("B", 3),
                                new MulticlassConfusionMatrix.PredictedClass("C", 9)
                            ),
                            3L
                        ),
                        new MulticlassConfusionMatrix.ActualClass(
                            "C",
                            17,
                            List.of(
                                new MulticlassConfusionMatrix.PredictedClass("A", 8),
                                new MulticlassConfusionMatrix.PredictedClass("B", 2),
                                new MulticlassConfusionMatrix.PredictedClass("C", 7)
                            ),
                            0L
                        )
                    ),
                    0
                )
            ),
            equalTo(
                List.of(
                    new PerClassSingleValue("A", 25.0 / 51),  // 13 false positives, 13 false negatives
                    new PerClassSingleValue("B", 26.0 / 51),  // 8 false positives, 17 false negatives
                    new PerClassSingleValue("C", 28.0 / 51)
                )
            ) // 13 false positives, 10 false negatives
        );
    }

    public void testComputePerClassAccuracy_OtherActualClassCountIsNonZero() {
        expectThrows(AssertionError.class, () -> Accuracy.computePerClassAccuracy(new MulticlassConfusionMatrix.Result(List.of(), 1)));
    }
}
