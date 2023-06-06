/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PATTERN;

/**
 * A service that allows the resolution of {@link AnalyticsCollection} by name.
 */
public class AnalyticsCollectionResolver {
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final ClusterService clusterService;

    @Inject
    public AnalyticsCollectionResolver(IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
    }

    /**
     * Resolves a collection by exact name and returns it.
     *
     * @param collectionName Collection name
     * @return The {@link AnalyticsCollection} object
     * @throws ResourceNotFoundException when no analytics collection is found.
     */
    public AnalyticsCollection collection(String collectionName) throws ResourceNotFoundException {
        return collection(clusterService.state(), collectionName);
    }

    /**
     * Resolves a collection by exact name and returns it.
     *
     * @param state Cluster state.
     * @param collectionName Collection name
     * @return The {@link AnalyticsCollection} object
     * @throws ResourceNotFoundException when no analytics collection is found.
     */
    public AnalyticsCollection collection(ClusterState state, String collectionName) throws ResourceNotFoundException {
        AnalyticsCollection collection = new AnalyticsCollection(collectionName);

        if (state.metadata().dataStreams().containsKey(collection.getEventDataStream()) == false) {
            throw new ResourceNotFoundException("no such analytics collection [{}]", collectionName);
        }

        return collection;
    }

    /**
     * Resolves one or several collection by expression and returns them as a list.
     * Expressions can be exact collection name but also contains wildcards.
     *
     * @param state Cluster state.
     * @param expressions Array of the collection name expressions to be matched.
     * @return List of {@link AnalyticsCollection} objects that match the expressions.
     * @throws ResourceNotFoundException when no analytics collection is found.
     */
    public List<AnalyticsCollection> collections(ClusterState state, String... expressions) {
        // Listing data streams that are matching the analytics collection pattern.
        List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
            state,
            IndicesOptions.lenientExpandOpen(),
            EVENT_DATA_STREAM_INDEX_PATTERN
        );

        Map<String, AnalyticsCollection> collections = dataStreams.stream()
            .map(AnalyticsCollection::fromDataStreamName)
            .filter(analyticsCollection -> matchAnyExpression(analyticsCollection, expressions))
            .collect(Collectors.toMap(AnalyticsCollection::getName, Function.identity()));

        List<String> missingCollections = Arrays.stream(expressions)
            .filter(not(Regex::isMatchAllPattern))
            .filter(not(Regex::isSimpleMatchPattern))
            .filter(not(collections::containsKey))
            .toList();

        if (missingCollections.isEmpty() == false) {
            throw new ResourceNotFoundException("no such analytics collection [{}] ", missingCollections.get(0));
        }

        return new ArrayList<>(collections.values());
    }

    private boolean matchExpression(String collectionName, String expression) {
        if (Strings.isNullOrEmpty(expression)) {
            return false;
        }

        if (Regex.isMatchAllPattern(expression)) {
            return true;
        }

        if (Regex.isSimpleMatchPattern(expression)) {
            return Regex.simpleMatch(expression, collectionName);
        }

        return collectionName.equals(expression);
    }

    private boolean matchAnyExpression(String collectionName, String... expressions) {
        if (expressions.length < 1) {
            return true;
        }

        return Arrays.stream(expressions).anyMatch(expression -> matchExpression(collectionName, expression));
    }

    private boolean matchAnyExpression(AnalyticsCollection collection, String... expressions) {
        return matchAnyExpression(collection.getName(), expressions);
    }
}
