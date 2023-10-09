/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

public class ConcurrentSearchSingleNodeTests extends ESSingleNodeTestCase {

    private final boolean concurrentSearch = randomBoolean();

    public void testConcurrentSearch() throws IOException {
        client().admin().indices().prepareCreate("index").get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        SearchService searchService = getInstanceFromNode(SearchService.class);
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shard.shardId(), 0L, AliasFilter.EMPTY);
        try (SearchContext searchContext = searchService.createSearchContext(shardSearchRequest, TimeValue.MINUS_ONE)) {
            ContextIndexSearcher searcher = searchContext.searcher();
            if (concurrentSearch) {
                assertEquals(1, searcher.getMinimumDocsPerSlice());
            } else {
                assertEquals(50_000, searcher.getMinimumDocsPerSlice());
            }
        }
    }

    @Override
    protected boolean enableConcurrentSearch() {
        return concurrentSearch;
    }
}
