/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * Defines an evaluation
 */
public interface Evaluation extends ToXContentObject, NamedWriteable {

    /**
     * Returns the evaluation name
     */
    String getName();

    /**
     * Builds the search required to collect data to compute the evaluation result
     * @param queryBuilder User-provided query that must be respected when collecting data
     */
    SearchSourceBuilder buildSearch(QueryBuilder queryBuilder);

    /**
     * Computes the evaluation result
     * @param searchResponse The search response required to compute the result
     * @param listener A listener of the results
     */
    void evaluate(SearchResponse searchResponse, ActionListener<List<EvaluationMetricResult>> listener);
}
