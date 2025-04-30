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
 * A no-op implementation of {@link ShardSearchLoadRateStats} that always returns
 * a default {@link SearchLoadRate} with zero values.
 * <p>
 * This implementation is useful as a fallback or default when no meaningful search load tracking
 * is needed, such as in testing environments or when load tracking is disabled.
 */
public class NoOpShardSearchLoadRateStats implements ShardSearchLoadRateStats {

    public NoOpShardSearchLoadRateStats() {}

    /**
     * Returns a {@link SearchLoadRate} instance with all values set to zero,
     * indicating no search load activity.
     *
     * @param stats the search statistics for the shard (ignored in this implementation)
     * @return a no-op {@code SearchLoadRate} with zeroed fields
     */
    @Override
    public SearchLoadRate getSearchLoadRate(SearchStats.Stats stats) {
        return SearchLoadRate.NO_OP;
    }
}
