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

    public void testDetachedShardContextDoesNotReleaseSearchContext() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));

        SearchContext normalSearchContext = newSearchContext(mapperService);
        ShardContext normalShardContext = new ComputeSearchContext(0, normalSearchContext).shardContext(QueryWarnings.EMIT);
        normalShardContext.decRef();
        assertTrue(normalSearchContext.isClosed());

        SearchContext retainedSearchContext = newSearchContext(mapperService);
        ComputeSearchContext retainedContext = new ComputeSearchContext(0, retainedSearchContext);
        ShardContext detachedShardContext = retainedContext.newDetachedShardContext();
        detachedShardContext.decRef();
        assertFalse(retainedSearchContext.isClosed());

        retainedContext.close();
        assertTrue(retainedSearchContext.isClosed());
    }

    private SearchContext newSearchContext(MapperService mapperService) {
        return new TestSearchContext(createSearchExecutionContext(mapperService, null));
    }
}
