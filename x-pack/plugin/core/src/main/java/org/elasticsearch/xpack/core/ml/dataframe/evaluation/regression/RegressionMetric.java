/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;

import java.util.List;

public interface RegressionMetric extends EvaluationMetric {

    /**
     * Builds the aggregation that collect required data to compute the metric
     * @param actualField the field that stores the actual value
     * @param predictedField the field that stores the predicted value
     * @return the aggregations required to compute the metric
     */
    List<AggregationBuilder> aggs(String actualField, String predictedField);

    /**
     * Processes given aggregations as a step towards computing result
     * @param aggs aggregations from {@link SearchResponse}
     */
    void process(Aggregations aggs);
}
