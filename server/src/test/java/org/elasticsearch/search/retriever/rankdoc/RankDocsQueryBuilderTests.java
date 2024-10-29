/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
        return new RankDocsQueryBuilder(rankDocs, null, false);
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

    public void testRankDocsQueryEarlyTerminate() throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter iw = new IndexWriter(directory, config)) {
                int seg = atLeast(5);
                int numDocs = atLeast(20);
                for (int i = 0; i < seg; i++) {
                    for (int j = 0; j < numDocs; j++) {
                        Document doc = new Document();
                        doc.add(new NumericDocValuesField("active", 1));
                        iw.addDocument(doc);
                    }
                    if (frequently()) {
                        iw.flush();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                int topSize = randomIntBetween(1, reader.maxDoc() / 5);
                RankDoc[] rankDocs = new RankDoc[topSize];
                int index = 0;
                for (int r : randomSample(random(), reader.maxDoc(), topSize)) {
                    rankDocs[index++] = new RankDoc(r, randomFloat(), randomIntBetween(0, 5));
                }
                Arrays.sort(rankDocs);
                for (int i = 0; i < rankDocs.length; i++) {
                    rankDocs[i].rank = i;
                }
                IndexSearcher searcher = new IndexSearcher(reader);
                for (int totalHitsThreshold = 0; totalHitsThreshold < reader.maxDoc(); totalHitsThreshold += randomIntBetween(1, 10)) {
                    // Tests that the query matches only the {@link RankDoc} when the hit threshold is reached.
                    RankDocsQuery q = new RankDocsQuery(
                        reader,
                        rankDocs,
                        new Query[] { NumericDocValuesField.newSlowExactQuery("active", 1) },
                        new String[1],
                        false
                    );
                    var topDocsManager = new TopScoreDocCollectorManager(topSize, null, totalHitsThreshold);
                    var col = searcher.search(q, topDocsManager);
                    // depending on the doc-ids of the RankDocs (i.e. the actual docs to have score) we could visit them last,
                    // so worst case is we could end up collecting up to 1 + max(topSize , totalHitsThreshold) + rankDocs.length documents
                    // as we could have already filled the priority queue with non-optimal docs
                    assertThat(
                        col.totalHits.value(),
                        lessThanOrEqualTo((long) (1 + Math.max(topSize, totalHitsThreshold) + rankDocs.length))
                    );
                    assertEqualTopDocs(col.scoreDocs, rankDocs);
                }

                {
                    // Return all docs (rank + tail)
                    RankDocsQuery q = new RankDocsQuery(
                        reader,
                        rankDocs,
                        new Query[] { NumericDocValuesField.newSlowExactQuery("active", 1) },
                        new String[1],
                        false
                    );
                    var topDocsManager = new TopScoreDocCollectorManager(topSize, null, Integer.MAX_VALUE);
                    var col = searcher.search(q, topDocsManager);
                    assertThat(col.totalHits.value(), equalTo((long) reader.maxDoc()));
                    assertEqualTopDocs(col.scoreDocs, rankDocs);
                }

                {
                    // Only rank docs
                    RankDocsQuery q = new RankDocsQuery(
                        reader,
                        rankDocs,
                        new Query[] { NumericDocValuesField.newSlowExactQuery("active", 1) },
                        new String[1],
                        true
                    );
                    var topDocsManager = new TopScoreDocCollectorManager(topSize, null, Integer.MAX_VALUE);
                    var col = searcher.search(q, topDocsManager);
                    assertThat(col.totalHits.value(), equalTo((long) topSize));
                    assertEqualTopDocs(col.scoreDocs, rankDocs);
                }

                {
                    // A single rank doc in the last segment
                    RankDoc[] singleRankDoc = new RankDoc[1];
                    singleRankDoc[0] = rankDocs[rankDocs.length - 1];
                    RankDocsQuery q = new RankDocsQuery(
                        reader,
                        singleRankDoc,
                        new Query[] { NumericDocValuesField.newSlowExactQuery("active", 1) },
                        new String[1],
                        false
                    );
                    var topDocsManager = new TopScoreDocCollectorManager(1, null, 0);
                    var col = searcher.search(q, topDocsManager);
                    assertThat(col.totalHits.value(), lessThanOrEqualTo((long) (2 + rankDocs.length)));
                    assertEqualTopDocs(col.scoreDocs, singleRankDoc);
                }
            }
        }
    }

    private static int[] randomSample(Random rand, int n, int k) {
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        for (int i = k; i < n; i++) {
            int j = rand.nextInt(i + 1);
            if (j < k) {
                reservoir[j] = i;
            }
        }
        return reservoir;
    }

    private static void assertEqualTopDocs(ScoreDoc[] scoreDocs, RankDoc[] rankDocs) {
        for (int i = 0; i < scoreDocs.length; i++) {
            assertEquals(rankDocs[i].doc, scoreDocs[i].doc);
            assertEquals(rankDocs[i].score, scoreDocs[i].score, 0f);
            assertEquals(-1, scoreDocs[i].shardIndex);
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
