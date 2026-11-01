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
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

import java.io.IOException;

public class ComputeSearchContextTests extends MapperServiceTestCase {

    /**
     * The shard context holds the last reference to the search context, so releasing it closes the search context.
     */
    public void testReleasingTheShardContextClosesTheSearchContext() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));
        SearchContext searchContext = new TestSearchContext(createSearchExecutionContext(mapperService, null));
        ShardContext shardContext = new ComputeSearchContext(0, searchContext).shardContext(QueryWarnings.EMIT);
        shardContext.decRef();
        assertTrue(searchContext.isClosed());
    }
}
