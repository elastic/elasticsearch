/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.util.List;

public interface RegressionMetric extends ToXContentObject, NamedWriteable {

    /**
     * Returns the name of the metric (which may differ to the writeable name)
     */
    String getMetricName();

    /**
     * Builds the aggregation that collect required data to compute the metric
     * @param actualField the field that stores the actual value
     * @param predictedField the field that stores the predicted value
     * @return the aggregations required to compute the metric
     */
    List<AggregationBuilder> aggs(String actualField, String predictedField);

    /**
     * Calculates the metric result
     * @param aggs the aggregations
     * @return the metric result
     */
    EvaluationMetricResult evaluate(Aggregations aggs);
}
