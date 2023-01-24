/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesQueryCache;

/**
 * The index-level query cache. This class mostly delegates to the node-level
 * query cache: {@link IndicesQueryCache}.
 */
public class IndexQueryCache implements QueryCache {

    private static final Logger logger = LogManager.getLogger(IndexQueryCache.class);

    private final IndicesQueryCache indicesQueryCache;

    protected final Index index;

    public IndexQueryCache(Index index, IndicesQueryCache indicesQueryCache) {
        this.indicesQueryCache = indicesQueryCache;
        this.index = index;
    }

    @Override
    public void close() throws ElasticsearchException {
        clear("close");
    }

    @Override
    public void clear(String reason) {
        logger.debug("full cache clear for [{}], reason [{}]", index, reason);
        indicesQueryCache.clearIndex(index.getName());
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return indicesQueryCache.doCache(weight, policy);
    }

}
