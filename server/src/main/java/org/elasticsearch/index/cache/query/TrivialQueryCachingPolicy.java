/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;

public enum TrivialQueryCachingPolicy implements QueryCachingPolicy {
    ALWAYS(true),
    NEVER(false);

    private final boolean shouldCache;

    TrivialQueryCachingPolicy(boolean shouldCache) {
        this.shouldCache = shouldCache;
    }

    @Override
    public void onUse(Query query) {}

    @Override
    public boolean shouldCache(Query query) {
        return shouldCache;
    }
}
