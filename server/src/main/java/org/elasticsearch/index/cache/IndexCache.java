/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;

import java.io.Closeable;
import java.io.IOException;

public class IndexCache implements Closeable {

    private final QueryCache queryCache;
    private final BitsetFilterCache bitsetFilterCache;

    public IndexCache(QueryCache queryCache, BitsetFilterCache bitsetFilterCache) {
        this.queryCache = queryCache;
        this.bitsetFilterCache = bitsetFilterCache;
    }

    public QueryCache query() {
        return queryCache;
    }

    /**
     * Return the {@link BitsetFilterCache} for this index.
     */
    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(queryCache, bitsetFilterCache);
    }

    public void clear(String reason) {
        queryCache.clear(reason);
        bitsetFilterCache.clear(reason);
    }

}
