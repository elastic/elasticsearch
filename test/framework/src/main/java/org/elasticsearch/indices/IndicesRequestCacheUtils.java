/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class IndicesRequestCacheUtils {
    private IndicesRequestCacheUtils() {
        // no instances of this class should be created
    }

    public static IndicesRequestCache getRequestCache(IndicesService indicesService) {
        return indicesService.indicesRequestCache;
    }

    public static Iterable<String> cachedKeys(IndicesRequestCache cache) {
        return StreamSupport.stream(cache.cachedKeys().spliterator(), false).map(Object::toString).collect(Collectors.toList());
    }

    public static void cleanCache(IndicesRequestCache cache) {
        cache.cleanCache();
    }
}
