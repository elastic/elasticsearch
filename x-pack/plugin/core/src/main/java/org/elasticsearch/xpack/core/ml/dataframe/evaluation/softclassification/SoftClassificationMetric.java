/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;

import java.util.List;

public interface SoftClassificationMetric extends EvaluationMetric {

    /**
     * The information of a specific class
     */
    interface ClassInfo {

        /**
         * Returns the class name
         */
        String getName();

        /**
         * Returns a query that matches documents of the class
         */
        QueryBuilder matchingQuery();

        /**
         * Returns the field that has the probability to be of the class
         */
        String getProbabilityField();
    }

    /**
     * Builds the aggregation that collect required data to compute the metric
     * @param actualField the field that stores the actual class
     * @param classInfos the information of each class to compute the metric for
     * @return the aggregations required to compute the metric
     */
    List<AggregationBuilder> aggs(String actualField, List<ClassInfo> classInfos);

    /**
     * Processes given aggregations as a step towards computing result
     * @param classInfo the class to calculate the metric for
     * @param aggs aggregations from {@link SearchResponse}
     */
    void process(ClassInfo classInfo, Aggregations aggs);
}
