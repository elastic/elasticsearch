/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

/** Verifies that capture retains rewritten queries without replacing the original query used for logging. */
public class SearchCaptureContextBuilderTests extends ESTestCase {
    public void testCaptureRewrittenSourceOnSuccess() {
        SearchRequest request = new SearchRequest("index").source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("semantic", "query text"))
        );
        var builder = newBuilder(request);
        String originalQuery = builder.build(new IllegalStateException()).getQuery();
        var vectorQuery = new KnnVectorQueryBuilder("semantic.embeddings", new float[] { 1.0f, 2.0f }, 1, 10, null, null, null);
        var rewrittenSource = new SearchSourceBuilder().query(vectorQuery);
        builder.captureRewrittenSource(rewrittenSource);
        // Later changes to the search source must not replace the captured query.
        rewrittenSource.query(new MatchNoneQueryBuilder());
        SearchResponse response = SearchResponse.emptyResponseBuilder().build();
        try {
            SearchCaptureContext context = builder.build(response);
            assertTrue(context.isSuccess());
            assertEquals(originalQuery, context.getQuery());
            assertSame(vectorQuery, context.getRewrittenSource().query());
            assertNotSame(rewrittenSource, context.getRewrittenSource());
        } finally {
            response.decRef();
        }
    }

    public void testFailureBeforeRewrite() {
        var context = newBuilder(new SearchRequest()).build(new IllegalStateException("rewrite failed"));
        assertNull(context.getRewrittenSource());
        assertFalse(context.isSuccess());
        assertEquals("rewrite failed", context.getErrorMessage());
    }

    public void testFailureAfterRewrite() {
        var builder = newBuilder(new SearchRequest());
        var query = QueryBuilders.termQuery("field", "value");
        builder.captureRewrittenSource(new SearchSourceBuilder().query(query));
        var context = builder.build(new IllegalStateException("search failed"));
        assertSame(query, context.getRewrittenSource().query());
        assertFalse(context.isSuccess());
        assertEquals("search failed", context.getErrorMessage());
    }

    public void testRequestWithoutSource() {
        var builder = newBuilder(new SearchRequest());
        builder.captureRewrittenSource(null);
        SearchResponse response = SearchResponse.emptyResponseBuilder().build();
        try {
            var context = builder.build(response);
            assertNull(context.getRewrittenSource());
            assertEquals("{}", context.getQuery());
        } finally {
            response.decRef();
        }
    }

    private static SearchCaptureContextBuilder newBuilder(SearchRequest request) {
        var task = new SearchTask(1, "transport", "search", () -> "search", TaskId.EMPTY_TASK_ID, Map.of());
        return new SearchCaptureContextBuilder(task, new NamedWriteableRegistry(List.of()), request);
    }
}
