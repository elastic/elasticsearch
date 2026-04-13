/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Per-cluster state for datafeed extraction when querying across multiple clusters
 * (e.g. cross-cluster search). Used to report status, optional error reason, and search
 * latency per cluster.
 */
public record LinkedClusterState(String alias, Status status, @Nullable String errorReason, long searchLatencyMs) {

    public LinkedClusterState {
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("[alias] cannot be empty");
        }
        Objects.requireNonNull(status);
    }

    /**
     * Status of a linked cluster in the search response, mapped from
     * {@link org.elasticsearch.action.search.SearchResponse.Cluster.Status}.
     */
    public enum Status {
        /** Cluster search completed successfully (mapped from SUCCESSFUL, RUNNING, or PARTIAL). */
        AVAILABLE,
        /** Cluster was skipped (e.g. skip_unavailable). */
        SKIPPED,
        /** Cluster search failed. */
        FAILED
    }
}
