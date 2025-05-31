/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

/**
 * Service interface for estimating the search load rate of a shard using provided search statistics.
 * <p>
 * Implementations may apply various heuristics or models, such as exponentially weighted moving rate,
 * to track and estimate the current load on a shard based on its search activity.
 */
public interface ShardSearchLoadRateService {

    /**
     * A no-op implementation of {@code ShardSearchLoadRateService} that always returns  {@code 0.0}
     * This can be used as a fallback or default when no actual load tracking is required.
     */
    ShardSearchLoadRateService NOOP = (stats) -> 0.0;

    /**
     * Computes the search load rate based on the provided shard-level search statistics.
     *
     * @param stats the search statistics for the shard, typically including metrics like query count, latency, etc.
     * @return the {@code double} representing the calculated EWMR on the shard
     */
    double getSearchLoadRate(SearchStats.Stats stats);
}
