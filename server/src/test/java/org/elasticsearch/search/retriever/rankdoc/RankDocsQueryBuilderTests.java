/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;

public class RankDocsQueryBuilderTests extends AbstractQueryTestCase<RankDocsQueryBuilder> {

    private RankDoc[] generateRandomRankDocs() {
        int totalDocs = randomIntBetween(0, 10);
        RankDoc[] rankDocs = new RankDoc[totalDocs];
        int currentDoc = 0;
        for (int i = 0; i < totalDocs; i++) {
            RankDoc rankDoc = new RankDoc(currentDoc, randomFloat(), randomIntBetween(0, 2));
            rankDocs[i] = rankDoc;
            currentDoc += randomIntBetween(0, 100);
        }
        return rankDocs;
    }

    @Override
    protected RankDocsQueryBuilder doCreateTestQueryBuilder() {
        RankDoc[] rankDocs = generateRandomRankDocs();
        return new RankDocsQueryBuilder(rankDocs);
    }

    @Override
    protected void doAssertLuceneQuery(RankDocsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertTrue(query instanceof RankDocsQuery);
        RankDocsQuery rankDocsQuery = (RankDocsQuery) query;
        assertArrayEquals(queryBuilder.rankDocs(), rankDocsQuery.rankDocs());
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testToQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                RankDocsQueryBuilder queryBuilder = createTestQueryBuilder();
                Query query = queryBuilder.doToQuery(context);

                assertTrue(query instanceof RankDocsQuery);
                RankDocsQuery rankDocsQuery = (RankDocsQuery) query;

                int shardIndex = context.getShardRequestIndex();
                int expectedDocs = (int) Arrays.stream(queryBuilder.rankDocs()).filter(x -> x.shardIndex == shardIndex).count();
                assertEquals(expectedDocs, rankDocsQuery.rankDocs().length);
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testCacheability() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                RankDocsQueryBuilder queryBuilder = createTestQueryBuilder();
                QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
                assertNotNull(rewriteQuery.toQuery(context));
                assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
            }
        }
    }

    /**
     * Overridden to ensure that {@link SearchExecutionContext} has a non-null {@link IndexReader}
     */
    @Override
    public void testMustRewrite() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(new Document());
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext context = createSearchExecutionContext(newSearcher(reader));
                context.setAllowUnmappedFields(true);
                RankDocsQueryBuilder queryBuilder = createTestQueryBuilder();
                queryBuilder.toQuery(context);
            }
        }
    }

    @Override
    public void testFromXContent() throws IOException {
        // no-op since RankDocsQueryBuilder is an internal only API
    }

    @Override
    public void testUnknownField() throws IOException {
        // no-op since RankDocsQueryBuilder is agnostic to unknown fields and an internal only API
    }

    @Override
    public void testValidOutput() throws IOException {
        // no-op since RankDocsQueryBuilder is an internal only API
    }
}
