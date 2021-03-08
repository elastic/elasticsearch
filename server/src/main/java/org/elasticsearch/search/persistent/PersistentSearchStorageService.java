/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

public class PersistentSearchStorageService {
    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);

    private final PersistentSearchResultsMemoryStore inMemoryCache;
    private final PersistentSearchResultsIndexStore indexStore;

    public PersistentSearchStorageService(PersistentSearchResultsMemoryStore inMemoryCache,
                                          PersistentSearchResultsIndexStore indexStore) {
        this.inMemoryCache = inMemoryCache;
        this.indexStore = indexStore;
    }

    public void storeShardResult(ShardSearchResult searchResult, ActionListener<String> storeListener) {
        inMemoryCache.put(searchResult);
        storeListener.onResponse(searchResult.getPersistentSearchId());
    }

    public void getShardResult(String id, ActionListener<ShardSearchResult> listener) {
        final ShardSearchResult shardSearchResult = inMemoryCache.remove(id);
        if (shardSearchResult != null) {
            listener.onResponse(shardSearchResult);
            return;
        }

        logger.debug("Unable to find shard search result with id [{}] in memory, falling back to index store", id);

        indexStore.getShardResult(id, listener);
    }

    public void storeResult(PersistentSearchResponse persistentSearchResponse, ActionListener<String> listener) {
        indexStore.storeResult(persistentSearchResponse, listener);
    }

    public void getPersistentSearchResponseAsync(String id, ActionListener<PersistentSearchResponse> listener) {
        indexStore.getPersistentSearchResponseAsync(id, listener);
    }
}
