/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests that {@link RootScopedQueryBuilder} builds its wrapped query at the root nested scope.
 */
public class RootScopedQueryBuilderTests extends MapperServiceTestCase {

    private MapperService mapperService;

    @Before
    public void initMapping() throws IOException {
        mapperService = createMapperService(mapping(b -> {
            b.startObject("foo").field("type", "keyword").endObject();
            b.startObject("num").field("type", "long").endObject();
            b.startObject("nested1").field("type", "nested");
            {
                b.startObject("properties");
                b.startObject("foo").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("nested2").field("type", "nested");
            {
                b.startObject("properties");
                b.startObject("foo").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
    }

    public void testToQueryBuildsAtRootScope() throws IOException {
        final CheckedConsumer<
            CheckedBiFunction<QueryBuilder, SearchExecutionContext, Query, IOException>,
            IOException> assertBuildsAtRootScope = toQuery -> {
                QueryBuilder inner = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "bar"), ScoreMode.Avg);
                SearchExecutionContext context = createSearchExecutionContext(mapperService);
                Query expected = toQuery.apply(inner, context);

                NestedObjectMapper nestedMapper = pushNestedScope(context, "nested2");
                assertNotEquals(expected, toQuery.apply(inner, context));
                assertEquals(expected, toQuery.apply(new RootScopedQueryBuilder(inner), context));
                assertSame(nestedMapper, context.nestedScope().getObjectMapper());
            };

        assertBuildsAtRootScope.accept(QueryBuilder::toQuery);
        assertBuildsAtRootScope.accept((query, context) -> query.toQuery(context, new MaxClauseCountQueryVisitor(1024)));
    }

    public void testScopeRestoredOnFailure() {
        RootScopedQueryBuilder rootScoped = new RootScopedQueryBuilder(new TermQueryBuilder("num", "not_a_number"));
        SearchExecutionContext context = createSearchExecutionContext(mapperService);

        NestedObjectMapper nestedMapper = pushNestedScope(context, "nested2");
        assertThrows(IllegalArgumentException.class, () -> rootScoped.toQuery(context));
        assertSame(nestedMapper, context.nestedScope().getObjectMapper());

        assertThrows(IllegalArgumentException.class, () -> rootScoped.toQuery(context, new MaxClauseCountQueryVisitor(1024)));
        assertSame(nestedMapper, context.nestedScope().getObjectMapper());
    }

    public void testRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext(mapperService);

        RootScopedQueryBuilder unchanged = new RootScopedQueryBuilder(new TermQueryBuilder("foo", "bar"));
        assertSame(unchanged, unchanged.rewrite(context));

        QueryBuilder changed = new RootScopedQueryBuilder(new BoolQueryBuilder()).rewrite(context);
        assertThat(changed, instanceOf(RootScopedQueryBuilder.class));
        assertThat(((RootScopedQueryBuilder) changed).query(), instanceOf(MatchAllQueryBuilder.class));

        QueryBuilder matchNone = new RootScopedQueryBuilder(new TermQueryBuilder("missing", "x")).rewrite(context);
        assertThat(matchNone, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testDelegation() {
        TermQueryBuilder inner = new TermQueryBuilder("foo", "bar");
        RootScopedQueryBuilder rootScoped = new RootScopedQueryBuilder(inner);

        assertSame(rootScoped, rootScoped.boost(2f));
        assertEquals(2f, inner.boost(), 0f);
        assertEquals(2f, rootScoped.boost(), 0f);
        assertSame(rootScoped, rootScoped.queryName("q"));
        assertEquals("q", inner.queryName());
        assertEquals("q", rootScoped.queryName());

        assertEquals(inner.getName(), rootScoped.getName());
        assertEquals(inner.getWriteableName(), rootScoped.getWriteableName());
        assertEquals(inner.getMinimalSupportedVersion(), rootScoped.getMinimalSupportedVersion());

        RootScopedQueryBuilder same = new RootScopedQueryBuilder(new TermQueryBuilder("foo", "bar").boost(2f).queryName("q"));
        assertEquals(rootScoped, same);
        assertEquals(rootScoped.hashCode(), same.hashCode());
        assertNotEquals(rootScoped, new RootScopedQueryBuilder(new TermQueryBuilder("foo", "baz")));
    }

    public void testSerializationUnwraps() throws IOException {
        TermQueryBuilder inner = new TermQueryBuilder("foo", "bar");
        RootScopedQueryBuilder rootScoped = new RootScopedQueryBuilder(inner);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());

        assertEquals(inner, copyNamedWriteable(rootScoped, registry, QueryBuilder.class));
        assertEquals(Strings.toString(inner), Strings.toString(rootScoped));
    }

    private static NestedObjectMapper pushNestedScope(SearchExecutionContext context, String path) {
        NestedObjectMapper nestedMapper = context.nestedLookup().getNestedMappers().get(path);
        assertNotNull(nestedMapper);

        context.nestedScope().nextLevel(nestedMapper);
        return nestedMapper;
    }
}
