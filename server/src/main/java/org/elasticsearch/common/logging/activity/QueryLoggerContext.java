/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Query loggger related context - common items for query loggers.
 */
public interface QueryLoggerContext {
    /**
     * Map cluster->status (for now).
     */
    default Map<String, String> getClusters() {
        return Map.of();
    }

    /**
     * Get query text.
     */
    String getQuery();

    /**
     * Number of results.
     */
    int getResultCount();

    /**
     * Indices used in the query.
     */
    String[] getIndices();

    default Collection<String> getRemoteClusterAliases(Map<String, String> clusters) {
        return clusters.keySet().stream().filter(alias -> alias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false).toList();
    }

    default Map<String, Long> getCountsByStatus(Map<String, String> clusters) {
        // Group by status and count how many statuses of each kind there are
        return clusters.values().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}
