/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.apache.lucene.search.join.ScoreMode;
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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Defines an evaluation
 */
public interface Evaluation extends ToXContentObject, NamedWriteable {

    /**
     * Returns the evaluation name
     */
    String getName();

    /**
     * Returns the collection of fields required by evaluation
     */
    EvaluationFields getFields();

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
        for (Tuple<String, String> requiredField : getFields().listAll()) {
            String fieldDescriptor = requiredField.v1();
            String field = requiredField.v2();
            if (field == null) {
                String metricNamesString =
                    metrics.stream()
                        .filter(m -> m.getRequiredFields().contains(fieldDescriptor))
                        .map(EvaluationMetric::getName)
                        .collect(joining(", "));
                if (metricNamesString.isEmpty() == false) {
                    throw ExceptionsHelper.badRequestException(
                        "[{}] must define [{}] as required by the following metrics [{}]",
                        getName(), fieldDescriptor, metricNamesString);
                }
            }
        }
        return metrics;
    }

    /**
     * Builds the search required to collect data to compute the evaluation result
     * @param userProvidedQueryBuilder User-provided query that must be respected when collecting data
     */
    default SearchSourceBuilder buildSearch(EvaluationParameters parameters, QueryBuilder userProvidedQueryBuilder) {
        Objects.requireNonNull(userProvidedQueryBuilder);
        BoolQueryBuilder boolQuery =
            QueryBuilders.boolQuery()
                // Verify existence of the actual field (which is always required)
                .filter(QueryBuilders.existsQuery(getFields().getActualField()));
        if (getFields().getPredictedField() != null) {
            // Verify existence of the predicted field if required for this evaluation
            boolQuery.filter(QueryBuilders.existsQuery(getFields().getPredictedField()));
        }
        if (getFields().getResultsNestedField() != null) {
            // Verify existence of the results nested field if required for this evaluation
            QueryBuilder resultsNestedFieldExistsQuery = QueryBuilders.existsQuery(getFields().getResultsNestedField());
            boolQuery.filter(
                QueryBuilders.nestedQuery(getFields().getResultsNestedField(), resultsNestedFieldExistsQuery, ScoreMode.None)
                    .ignoreUnmapped(true));
        }
        if (getFields().getPredictedClassNameField() != null) {
            assert getFields().getResultsNestedField() != null;
            // Verify existence of the predicted class name field if required for this evaluation
            QueryBuilder classNameFieldExistsQuery = QueryBuilders.existsQuery(getFields().getPredictedClassNameField());
            boolQuery.filter(
                QueryBuilders.nestedQuery(getFields().getResultsNestedField(), classNameFieldExistsQuery, ScoreMode.None)
                    .ignoreUnmapped(true));
        }
        if (getFields().getPredictedProbabilityField() != null) {
            // Verify existence of the predicted probability field if required for this evaluation
            QueryBuilder probabilityFieldExistsQuery = QueryBuilders.existsQuery(getFields().getPredictedProbabilityField());
            // predicted probability field may be either nested (just like in case of classification evaluation) or non-nested (just like
            // in case of outlier detection evaluation). Here we support both modes.
            if (getFields().getResultsNestedField() != null) {
                boolQuery.filter(
                    QueryBuilders.nestedQuery(getFields().getResultsNestedField(), probabilityFieldExistsQuery, ScoreMode.None)
                        .ignoreUnmapped(true));
            } else {
                boolQuery.filter(probabilityFieldExistsQuery);
            }
        }
        // Apply user-provided query
        boolQuery.filter(userProvidedQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(boolQuery);
        for (EvaluationMetric metric : getMetrics()) {
            // Fetch aggregations requested by individual metrics
            Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs = metric.aggs(parameters, getFields());
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
            String requiredFieldsString =
                getFields().listAll().stream()
                    .map(Tuple::v2)
                    .filter(Objects::nonNull)
                    .collect(joining(", "));
            throw ExceptionsHelper.badRequestException("No documents found containing all the required fields [{}]", requiredFieldsString);
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
            .collect(toList());
    }
}
