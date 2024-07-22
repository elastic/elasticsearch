/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

@LuceneTestCase.AwaitsFix(bugUrl = "todo: this is somewhat meaningless with without a worker pool")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ConcurrentSearchTestPluginTests extends ESIntegTestCase {

    private final boolean concurrentSearch = randomBoolean();

    public void testConcurrentSearch() throws IOException {
        client().admin().indices().prepareCreate("index").get();
        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        SearchService searchService = internalCluster().getDataNodeInstance(SearchService.class);
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
