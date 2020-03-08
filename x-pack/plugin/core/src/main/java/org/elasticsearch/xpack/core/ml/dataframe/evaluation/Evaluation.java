/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
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
     * Returns the field containing the actual value
     */
    String getActualField();

    /**
     * Returns the field containing the predicted value
     */
    String getPredictedField();

    /**
     * Returns the list of metrics to evaluate
     * @return list of metrics to evaluate
     */
    List<? extends EvaluationMetric> getMetrics();

    default <T extends EvaluationMetric> List<T> initMetrics(@Nullable List<T> parsedMetrics, Supplier<List<T>> defaultMetricsSupplier) {
        List<T> metrics = parsedMetrics == null ? defaultMetricsSupplier.get() : new ArrayList<>(parsedMetrics);
        if (metrics.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must have one or more metrics", getName());
        }
        Collections.sort(metrics, Comparator.comparing(EvaluationMetric::getName));
        return metrics;
    }

    /**
     * Builds the search required to collect data to compute the evaluation result
     * @param userProvidedQueryBuilder User-provided query that must be respected when collecting data
     */
    default SearchSourceBuilder buildSearch(QueryBuilder userProvidedQueryBuilder) {
        Objects.requireNonNull(userProvidedQueryBuilder);
        BoolQueryBuilder boolQuery =
            QueryBuilders.boolQuery()
                // Verify existence of required fields
                .filter(QueryBuilders.existsQuery(getActualField()))
                .filter(QueryBuilders.existsQuery(getPredictedField()))
                // Apply user-provided query
                .filter(userProvidedQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(boolQuery);
        for (EvaluationMetric metric : getMetrics()) {
            // Fetch aggregations requested by individual metrics
            Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs = metric.aggs(getActualField(), getPredictedField());
            aggs.v1().forEach(searchSourceBuilder::aggregation);
            aggs.v2().forEach(searchSourceBuilder::aggregation);
        }
        return searchSourceBuilder;
    }

    /**
     * Processes {@link SearchResponse} from the search action
     * @param searchResponse response from the search action
     */
    default void process(SearchResponse searchResponse) {
        Objects.requireNonNull(searchResponse);
        if (searchResponse.getHits().getTotalHits().value == 0) {
            throw ExceptionsHelper.badRequestException(
                "No documents found containing both [{}, {}] fields", getActualField(), getPredictedField());
        }
        for (EvaluationMetric metric : getMetrics()) {
            metric.process(searchResponse.getAggregations());
        }
    }

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
