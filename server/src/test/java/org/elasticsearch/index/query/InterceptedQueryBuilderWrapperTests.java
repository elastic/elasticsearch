/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class InterceptedQueryBuilderWrapperTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;

    @Before
    public void setup() {
        threadPool = createThreadPool();
        client = new NoOpClient(threadPool);
    }

    @After
    public void cleanup() {
        threadPool.close();
    }

    public void testQueryNameReturnsWrappedQueryBuilder() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        String queryName = randomAlphaOfLengthBetween(5, 10);
        QueryBuilder namedQuery = interceptedQueryBuilderWrapper.queryName(queryName);
        assertTrue(namedQuery instanceof InterceptedQueryBuilderWrapper);
        assertEquals(queryName, namedQuery.queryName());
    }

    public void testQueryBoostReturnsWrappedQueryBuilder() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        float boost = randomFloat();
        QueryBuilder boostedQuery = interceptedQueryBuilderWrapper.boost(boost);
        assertTrue(boostedQuery instanceof InterceptedQueryBuilderWrapper);
        assertEquals(boost, boostedQuery.boost(), 0.0001f);
    }

    public void testRewrite() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(null, client, null);
        context.setQueryRewriteInterceptor(myMatchInterceptor);

        // Queries that are not intercepted behave normally
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("field", "value");
        QueryBuilder rewritten = termQueryBuilder.rewrite(context);
        assertTrue(rewritten instanceof TermQueryBuilder);

        // Queries that should be intercepted are and the right thing happens
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("field", "value");
        rewritten = matchQueryBuilder.rewrite(context);
        assertTrue(rewritten instanceof InterceptedQueryBuilderWrapper);
        assertTrue(((InterceptedQueryBuilderWrapper) rewritten).queryBuilder instanceof MatchQueryBuilder);
        MatchQueryBuilder rewrittenMatchQueryBuilder = (MatchQueryBuilder) ((InterceptedQueryBuilderWrapper) rewritten).queryBuilder;
        assertEquals("intercepted", rewrittenMatchQueryBuilder.value());

        // An additional rewrite on an already intercepted query returns the same query
        QueryBuilder rewrittenAgain = rewritten.rewrite(context);
        assertTrue(rewrittenAgain instanceof InterceptedQueryBuilderWrapper);
        assertEquals(rewritten, rewrittenAgain);
    }

    private final QueryRewriteInterceptor myMatchInterceptor = new QueryRewriteInterceptor() {
        @Override
        public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
            if (queryBuilder instanceof MatchQueryBuilder matchQueryBuilder) {
                return new MatchQueryBuilder(matchQueryBuilder.fieldName(), "intercepted");
            }
            return queryBuilder;
        }

        @Override
        public String getQueryName() {
            return MatchQueryBuilder.NAME;
        }
    };
}
