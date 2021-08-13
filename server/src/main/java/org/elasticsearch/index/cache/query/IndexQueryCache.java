/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.query;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesQueryCache;

/**
 * The index-level query cache. This class mostly delegates to the node-level
 * query cache: {@link IndicesQueryCache}.
 */
public class IndexQueryCache extends AbstractIndexComponent implements QueryCache {

    final IndicesQueryCache indicesQueryCache;

    public IndexQueryCache(IndexSettings indexSettings, IndicesQueryCache indicesQueryCache) {
        super(indexSettings);
        this.indicesQueryCache = indicesQueryCache;
    }

    @Override
    public void close() throws ElasticsearchException {
        clear("close");
    }

    @Override
    public void clear(String reason) {
        logger.debug("full cache clear, reason [{}]", reason);
        indicesQueryCache.clearIndex(index().getName());
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return indicesQueryCache.doCache(weight, policy);
    }

}
