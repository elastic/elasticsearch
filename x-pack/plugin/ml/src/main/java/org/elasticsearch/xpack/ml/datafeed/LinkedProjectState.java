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
 * Per-linked-project state for datafeed extraction when querying across multiple projects
 * (e.g. cross-project search). Used to report status, optional error reason, and search
 * latency per cluster/project.
 */
public record LinkedProjectState(String alias, Status status, @Nullable String errorReason, long searchLatencyMs) {

    public LinkedProjectState {
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("[alias] cannot be empty");
        }
        Objects.requireNonNull(status);
    }

    /**
     * Status of a linked project in the search response, mapped from
     * {@link org.elasticsearch.action.search.SearchResponse.Cluster.Status}.
     */
    public enum Status {
        /** Cluster/project search completed successfully (mapped from SUCCESSFUL, or PARTIAL). */
        AVAILABLE,
        /** Cluster/project was skipped (e.g. skip_unavailable). */
        SKIPPED,
        /** Cluster/project search failed. */
        FAILED
    }
}
