/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Query logger related context — common items for query loggers.
 */
public abstract class QueryLoggerContext extends ActivityLoggerContext {
    // Cached "isSystem" flag
    private Boolean isSystemSearch = null;

    protected QueryLoggerContext(Task task, String type, long tookInNanos, @Nullable Exception error) {
        super(task, type, tookInNanos, error);
    }

    protected QueryLoggerContext(Task task, String type, long tookInNanos) {
        super(task, type, tookInNanos);
    }

    /**
     * Map cluster->status (for now).
     */
    public Map<String, String> getClusters() {
        return Map.of();
    }

    /**
     * Get query text.
     */
    public abstract String getQuery();

    /**
     * Number of results.
     */
    public abstract int getResultCount();

    /**
     * Indices used in the query.
     */
    public abstract String[] getIndices();

    public boolean isSystemSearch(Predicate<String> systemChecker) {
        if (systemChecker == null) {
            return false;
        }
        if (isSystemSearch != null) {
            return isSystemSearch;
        }

        final String[] indices = getIndices();
        // Request that only asks for system indices is system search
        isSystemSearch = indices != null && indices.length > 0 && Arrays.stream(indices).allMatch(systemChecker);
        return isSystemSearch;
    }

    public Collection<String> getRemoteClusterAliases(Map<String, String> clusters) {
        return clusters.keySet().stream().filter(alias -> alias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false).toList();
    }

    public Map<String, Long> getCountsByStatus(Map<String, String> clusters) {
        // Group by status and count how many statuses of each kind there are
        return clusters.values().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}
