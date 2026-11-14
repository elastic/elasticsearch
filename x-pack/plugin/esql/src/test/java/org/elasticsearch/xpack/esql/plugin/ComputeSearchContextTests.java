/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ComputeSearchContextTests extends MapperServiceTestCase {

    /**
     * The shard context holds the last reference to the search context, so releasing it closes the search context.
     */
    public void testReleasingTheShardContextClosesTheSearchContext() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));
        SearchContext searchContext = new TestSearchContext(createSearchExecutionContext(mapperService, null));
        ShardContext shardContext = new ComputeSearchContext(0, searchContext, new LongAdder()).shardContext(
            QueryWarnings.EMIT,
            Set.of(),
            false
        );
        shardContext.decRef();
        assertTrue(searchContext.isClosed());
    }

    /**
     * The rows the sources emitted from a shard are kept by the contexts of the request once the shard context is released.
     */
    public void testRowsEmittedAreKeptAfterRelease() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));
        ShardId shardId = new ShardId("idx", "uuid", 0);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        SearchContext searchContext = new TestSearchContext(createSearchExecutionContext(mapperService, null), indexShard, null);
        try (AcquiredSearchContexts contexts = new AcquiredSearchContexts(1)) {
            ShardContext shardContext = contexts.newSubRangeView(List.of(searchContext))
                .get(0)
                .shardContext(QueryWarnings.EMIT, Set.of(), false);
            shardContext.addRowsEmitted(3);
            shardContext.addRowsEmitted(4);
            shardContext.decRef();
            assertTrue(searchContext.isClosed());
            assertEquals(7, contexts.rowsEmitted(shardId));
            assertEquals(0, contexts.rowsEmitted(new ShardId("idx", "uuid", 1)));
        }
    }
}
