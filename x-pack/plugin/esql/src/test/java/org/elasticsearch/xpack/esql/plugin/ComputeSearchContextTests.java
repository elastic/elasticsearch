/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.sameInstance;

public class ComputeSearchContextTests extends MapperServiceTestCase {

    public void testDetachedShardContextDoesNotReleaseSearchContext() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));

        SearchContext normalSearchContext = newSearchContext(mapperService);
        ShardContext normalShardContext = new ComputeSearchContext(0, normalSearchContext).shardContext(QueryWarnings.EMIT);
        normalShardContext.decRef();
        assertTrue(normalSearchContext.isClosed());

        SearchContext retainedSearchContext = newSearchContext(mapperService);
        ComputeSearchContext retainedContext = new ComputeSearchContext(0, retainedSearchContext);
        ShardContext detachedShardContext = retainedContext.newDetachedShardContext(QueryWarnings.EMIT);
        detachedShardContext.decRef();
        assertFalse(retainedSearchContext.isClosed());

        retainedContext.close();
        assertTrue(retainedSearchContext.isClosed());
    }

    public void testDetachedShardContextUsesRequestedQueryWarnings() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("k").field("type", "keyword").endObject()));

        try (ComputeSearchContext context = new ComputeSearchContext(0, newSearchContext(mapperService))) {
            assertQueryWarnings(context.newDetachedShardContext(QueryWarnings.EMIT), QueryWarnings.EMIT);
            assertQueryWarnings(context.newDetachedShardContext(QueryWarnings.NOOP), QueryWarnings.NOOP);
        }
    }

    private static void assertQueryWarnings(ShardContext shardContext, QueryWarnings expected) {
        AtomicReference<QueryWarnings> actual = new AtomicReference<>();
        try (shardContext) {
            shardContext.toQuery(new DummyQueryBuilder() {
                @Override
                protected Query doToQuery(SearchExecutionContext context) {
                    actual.set(((EsqlSearchExecutionContext) context).queryWarnings());
                    return new MatchAllDocsQuery();
                }
            });
        }
        assertThat(actual.get(), sameInstance(expected));
    }

    private SearchContext newSearchContext(MapperService mapperService) {
        return new TestSearchContext(createSearchExecutionContext(mapperService, null));
    }
}
