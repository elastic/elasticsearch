/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.ingest.useragent.UserAgentParser.Details;

class UserAgentCache {
    private final Cache<CompositeCacheKey, Details> cache;

    UserAgentCache(long cacheSize) {
        cache = CacheBuilder.<CompositeCacheKey, Details>builder().setMaximumWeight(cacheSize).build();
    }

    public Details get(String parserName, String userAgent) {
        return cache.get(new CompositeCacheKey(parserName, userAgent));
    }

    public void put(String parserName, String userAgent, Details details) {
        cache.put(new CompositeCacheKey(parserName, userAgent), details);
    }

    private record CompositeCacheKey(String parserName, String userAgent) {}
}
