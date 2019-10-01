/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Defines an evaluation
 */
public interface Evaluation extends ToXContentObject, NamedWriteable {

    /**
     * Returns the evaluation name
     */
    String getName();

    /**
     * Returns the list of metrics to evaluate
     * @return list of metrics to evaluate
     */
    List<? extends EvaluationMetric> getMetrics();

    /**
     * Builds the search required to collect data to compute the evaluation result
     * @param userProvidedQueryBuilder User-provided query that must be respected when collecting data
     */
    SearchSourceBuilder buildSearch(QueryBuilder userProvidedQueryBuilder);

    /**
     * Builds the search that verifies existence of required fields and applies user-provided query
     * @param requiredFields fields that must exist
     * @param userProvidedQueryBuilder user-provided query
     */
    default SearchSourceBuilder newSearchSourceBuilder(List<String> requiredFields, QueryBuilder userProvidedQueryBuilder) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String requiredField : requiredFields) {
            boolQuery.filter(QueryBuilders.existsQuery(requiredField));
        }
        boolQuery.filter(userProvidedQueryBuilder);
        return new SearchSourceBuilder().size(0).query(boolQuery);
    }

    /**
     * Processes {@link SearchResponse} from the search action
     * @param searchResponse response from the search action
     */
    void process(SearchResponse searchResponse);

    /**
     * @return true iff all the metrics have their results computed
     */
    default boolean hasAllResults() {
        return getMetrics().stream().map(EvaluationMetric::getResult).allMatch(Optional::isPresent);
    }

    /**
     * Returns the list of evaluation results
     * @return list of evaluation results
     */
    default List<EvaluationMetricResult> getResults() {
        return getMetrics().stream()
            .map(EvaluationMetric::getResult)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }
}
