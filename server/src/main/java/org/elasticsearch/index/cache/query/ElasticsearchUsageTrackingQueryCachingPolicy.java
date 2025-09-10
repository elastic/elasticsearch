/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.elasticsearch.search.vectors.CachingEnableFilterQuery;

/**
 * Elasticsearch-specific extension of Lucene's {@link UsageTrackingQueryCachingPolicy}.
 * <p>
 * This policy customizes the default usage-tracking query caching behavior to better
 * integrate with Elasticsearch's query execution model and caching strategy.
 * </p>
 */
public class ElasticsearchUsageTrackingQueryCachingPolicy extends UsageTrackingQueryCachingPolicy {
    public ElasticsearchUsageTrackingQueryCachingPolicy() {
        super(256);
    }

    protected int minFrequencyToCache(Query query) {
        int minFrequency = super.minFrequencyToCache(query);
        if (query instanceof CachingEnableFilterQuery) {
            // Cache this wrapper early, as it indicates the query will be eagerly evaluated.
            return --minFrequency;
        }
        return minFrequency;
    }
}
