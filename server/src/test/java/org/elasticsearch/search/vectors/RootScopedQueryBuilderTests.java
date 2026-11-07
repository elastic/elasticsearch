/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.junit.Before;

import java.io.IOException;

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
                b.startObject("deep").field("type", "nested");
                {
                    b.startObject("properties");
                    b.startObject("foo").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
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
                QueryBuilder inner = new NestedQueryBuilder("nested1.deep", new TermQueryBuilder("nested1.deep.foo", "bar"), ScoreMode.Avg);
                SearchExecutionContext context = createSearchExecutionContext(mapperService);
                Query expected = toQuery.apply(inner, context);

                // in the scope of an ancestor of its path, the nested query joins to that ancestor instead of the root
                NestedObjectMapper nestedMapper = pushNestedScope(context, "nested1");
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
        QueryRewriteContext context = new InnerHitsRewriteContext(parserConfig(), System::currentTimeMillis);

        RootScopedQueryBuilder unchanged = new RootScopedQueryBuilder(new TermQueryBuilder("foo", "bar"));
        assertSame(unchanged, unchanged.rewrite(context));

        QueryBuilder changed = new RootScopedQueryBuilder(new BoolQueryBuilder()).rewrite(context);
        assertThat(changed, instanceOf(RootScopedQueryBuilder.class));
        assertThat(((RootScopedQueryBuilder) changed).query(), instanceOf(MatchAllQueryBuilder.class));

        assertThat(new RootScopedQueryBuilder(new MatchNoneQueryBuilder()).rewrite(context), instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testTransient() {
        RootScopedQueryBuilder rootScoped = new RootScopedQueryBuilder(new TermQueryBuilder("foo", "bar"));
        expectThrows(IllegalStateException.class, () -> rootScoped.writeTo(new BytesStreamOutput()));
        expectThrows(IllegalStateException.class, () -> rootScoped.rewrite(createSearchExecutionContext(mapperService)));
    }

    private static NestedObjectMapper pushNestedScope(SearchExecutionContext context, String path) {
        NestedObjectMapper nestedMapper = context.nestedLookup().getNestedMappers().get(path);
        assertNotNull(nestedMapper);

        context.nestedScope().nextLevel(nestedMapper);
        return nestedMapper;
    }
}
