/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@link EvaluationMetric} class represents a metric to evaluate.
 */
public interface EvaluationMetric extends ToXContentObject, NamedWriteable {

    /**
     * Returns the name of the metric (which may differ to the writeable name)
     */
    String getName();

    /**
     * Returns the set of fields that this metric requires in order to be calculated.
     */
    Set<String> getRequiredFields();

    /**
     * Builds the aggregation that collect required data to compute the metric
     * @param parameters settings that may be needed by aggregations
     * @param fields fields that may be needed by aggregations
     * @return the aggregations required to compute the metric
     */
    Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters, EvaluationFields fields);

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
