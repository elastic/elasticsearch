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
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;

public class DisabledQueryCache extends AbstractIndexComponent implements QueryCache {

    public DisabledQueryCache(IndexSettings indexSettings) {
        super(indexSettings);
        logger.debug("Using no query cache");
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return weight;
    }

    @Override
    public void clear(String reason) {
        // nothing to do here
    }
}
