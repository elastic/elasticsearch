/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.util.List;
import java.util.Optional;

/**
 * {@link EvaluationMetric} class represents a metric to evaluate.
 */
public interface EvaluationMetric extends ToXContentObject, NamedWriteable {

    /**
     * Returns the name of the metric (which may differ to the writeable name)
     */
    String getName();

    /**
     * Builds the aggregation that collect required data to compute the metric
     * @param parameters settings that may be needed by aggregations
     * @param actualField the field that stores the actual value
     * @param predictedField the field that stores the predicted value (class name or probability)
     * @return the aggregations required to compute the metric
     */
    Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                           String actualField,
                                                                           String predictedField);

    /**
     * Processes given aggregations as a step towards computing result
     * @param aggs aggregations from {@link SearchResponse}
     */
    void process(Aggregations aggs);

    /**
     * Gets the evaluation result for this metric.
     * @return {@code Optional.empty()} if the result is not available yet, {@code Optional.of(result)} otherwise
     */
    Optional<? extends EvaluationMetricResult> getResult();
}
