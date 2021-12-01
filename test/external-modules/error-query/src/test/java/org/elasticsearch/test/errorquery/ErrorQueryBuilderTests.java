/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.errorquery;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ErrorQueryBuilderTests extends AbstractQueryTestCase<ErrorQueryBuilder> {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ErrorQueryPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected ErrorQueryBuilder doCreateTestQueryBuilder() {
        int numIndex = randomIntBetween(0, 5);
        List<IndexError> indices = new ArrayList<>();
        for (int i = 0; i < numIndex; i++) {
            String indexName = randomAlphaOfLengthBetween(5, 30);
            int numShards = randomIntBetween(0, 3);
            int[] shardIds = numShards > 0 ? new int[numShards] : null;
            for (int j = 0; j < numShards; j++) {
                shardIds[j] = j;
            }
            indices.add(
                new IndexError(indexName, shardIds, randomFrom(IndexError.ERROR_TYPE.values()), randomAlphaOfLengthBetween(5, 100))
            );
        }

        return new ErrorQueryBuilder(indices);
    }

    @Override
    protected void doAssertLuceneQuery(ErrorQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertEquals(new MatchAllDocsQuery(), query);
    }

    @Override
    public void testCacheability() throws IOException {
        ErrorQueryBuilder queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }
}
