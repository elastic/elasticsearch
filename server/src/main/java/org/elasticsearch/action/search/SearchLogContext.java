/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.activity.ActivityLoggerContext;
import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SearchLogContext extends ActivityLoggerContext implements QueryLoggerContext {
    public static final String TYPE = "dsl";
    private final SearchRequest request;
    private final @Nullable SearchResponse response;
    private final NamedWriteableRegistry namedWriteableRegistry;
    // Cached index names
    private String[] indexNames = null;
    // Cached "isSystem" flag
    private Boolean isSystemSearch = null;

    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private SearchLogContext(
        Task task,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchRequest request,
        @Nullable SearchResponse response,
        long tookInNanos,
        Exception error
    ) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = response;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    SearchLogContext(
        Task task,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchRequest request,
        long tookInNanos,
        SearchResponse response
    ) {
        this(task, namedWriteableRegistry, request, response, tookInNanos, null);
    }

    SearchLogContext(Task task, NamedWriteableRegistry namedWriteableRegistry, SearchRequest request, long tookInNanos, Exception error) {
        this(task, namedWriteableRegistry, request, null, tookInNanos, error);
    }

    @Override
    public String getQuery() {
        var source = request.source();
        if (source == null) {
            return "{}";
        } else {
            return source.toString(FORMAT_PARAMS);
        }
    }

    @Override
    public int getResultCount() {
        if (response == null || response.getHits() == null || response.getHits().getHits() == null) {
            return 0;
        }
        return response.getHits().getHits().length;
    }

    TotalHits getTotalHits() {
        if (response == null || response.getHits() == null) {
            return null;
        }
        return response.getHits().getTotalHits();
    }

    String[] getIndexNames() {
        if (indexNames == null) {
            if (request.pointInTimeBuilder() == null) {
                indexNames = request.indices();
            } else {
                final SearchContextId searchContextId = request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry);
                indexNames = searchContextId.getActualIndices();
            }
        }
        return indexNames;
    }

    boolean isSystemSearch(Predicate<String> systemChecker) {
        if (isSystemSearch == null) {
            isSystemSearch = false;
            String[] indices = getIndexNames();
            // Request that only asks for system indices is system search
            if (indices.length > 0 && Arrays.stream(indices).allMatch(systemChecker)) {
                isSystemSearch = true;
            }
        }
        return isSystemSearch;
    }

    @Override
    public boolean isTimedOut() {
        return response != null && response.isTimedOut();
    }

    @Override
    public String[] getIndices() {
        return getIndexNames();
    }

    public boolean hasAggregations() {
        return response != null && response.hasAggregations();
    }

    @Override
    public Optional<ShardInfo> shardInfo() {
        return Optional.ofNullable(response)
            .map(response -> new ShardInfo(response.getSuccessfulShards(), response.getSkippedShards(), response.getFailedShards()));
    }

    // CCS stuff

    /**
     * Non-null alias means the request is from a remote cluster.
     */
    public boolean isFromRemote() {
        return request.getLocalClusterAlias() != null;
    }

    @Override
    public Map<String, String> getClusters() {
        if (response == null) {
            return Map.of();
        }
        var clusters = response.getClusters();
        return clusters.getClusterAliases()
            .stream()
            .collect(Collectors.toMap(Function.identity(), alias -> clusters.getCluster(alias).getStatus().toString()));
    }
}
