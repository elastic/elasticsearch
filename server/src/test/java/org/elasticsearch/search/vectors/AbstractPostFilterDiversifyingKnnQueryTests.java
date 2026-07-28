/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Exercises the {@link PostFilterKnnQuery} orchestrator with diversifying children kNN queries.
 * Each logical document is indexed as a child/parent block: the child carries the vector and the
 * filter field ("tag"); the parent carries only the "_parent" marker used by the parents filter.
 * <p>
 * Unlike {@link AbstractPostFilterKnnQueryTests}, the diversifying queries return <b>child</b> doc
 * ids (the best-scoring child per parent) - see Lucene's {@code DiversifyingChildrenFloatKnnVectorQuery}.
 * With one child per parent the doc id of block {@code i} is {@code 2*i} (even) for the child and
 * {@code 2*i+1} (odd) for the parent, so all asserted result ids are even.
 */
abstract class AbstractPostFilterDiversifyingKnnQueryTests extends ESTestCase {

    protected abstract AssertingKnnQuery.VectorType vectorType();

    protected abstract void addVectorField(Document doc, String field, float value);

    /**
     * Indexes a single child/parent block. The child carries the vector and the "tag" filter
     * field; the parent (indexed last, as required by block joins) carries the "_parent" marker.
     * The child gets the even doc id {@code 2*blockOrdinal}, the parent the odd id {@code 2*blockOrdinal+1}.
     */
    private void indexDocBlock(IndexWriter writer, String field, float vectorValue, String tag) throws IOException {
        Document child = new Document();
        addVectorField(child, field, vectorValue);
        child.add(new KeywordField("tag", tag, Field.Store.NO));
        Document parent = new Document();
        parent.add(new KeywordField("docType", "_parent", Field.Store.NO));
        writer.addDocuments(List.of(child, parent));
    }

    private static BitSetProducer parentFilter(IndexReader reader) throws IOException {
        BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        CheckJoinIndex.check(reader, parentsFilter);
        return parentsFilter;
    }

    private AssertingKnnQuery createQuery(
        String field,
        float[] queryVector,
        int k,
        int numCands,
        Query filter,
        float postFilterScale,
        BitSetProducer parentsFilter
    ) {
        return new AssertingKnnQuery(vectorType(), field, queryVector, k, numCands, filter, postFilterScale, parentsFilter);
    }

    /**
     * 8 logical docs (16 Lucene docs), all tagged "pass". The delegate collects all children,
     * all parents pass the filter, and the top k=4 children are returned without retry.
     */
    public void testRound0AloneSatisfiesK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                indexDocBlock(writer, "vector", (float) i, "pass");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 2, 4, 6 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(1.0f, meta.postFilterDelegateSelectivity(), 0.001f);
            }
        }
    }

    /**
     * 8 logical docs: 4 pass (vectors 0..3, children 0/2/4/6), 4 fail (vectors 10..13). With k=4,
     * numCands=3, and scale=0.5, the delegate collects the 3 closest children {0,2,4}. All 3 pass,
     * but k=4 needs 1 more. Retry excludes {0,2,4} and finds child 6 (vector 3.0), closing the gap.
     */
    public void testRetryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                indexDocBlock(writer, "vector", (float) i, "pass");
            }
            for (int i = 0; i < 4; i++) {
                indexDocBlock(writer, "vector", 10f + i, "fail");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                int numCands = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 2, 4, 6 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.5f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0, 2, 4 }, meta.retryExcludedDocs());
                assertArrayEquals(new int[][] { { 0, 2, 4 } }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
            }
        }
    }

    /**
     * Two segments of 30 logical docs each (60 Lucene docs per segment). Each segment has 2 passing
     * children near the query vector and 2 passing children far away. The first pass finds only 4
     * passers total, which is less than k=5, so a retry fires with seed docs (child ids) spanning
     * both segments. The retry cannot reach the far passers, so the query falls through to the bare
     * inner diversifying query, which returns the 4 passers per leaf and merges to k=5.
     */
    public void testRetrySeedDocsSpanMultipleSegments() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 30; i++) {
                boolean pass = (i <= 1 || i >= 28);
                indexDocBlock(writer, "vector", (float) i, pass ? "pass" : "fail");
            }
            writer.commit();
            for (int i = 0; i < 30; i++) {
                boolean pass = (i <= 1 || i >= 28);
                indexDocBlock(writer, "vector", 50f + i, pass ? "pass" : "fail");
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals("expected two segments", 2, reader.leaves().size());
                int k = 5;
                int numCands = 5;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[][] { { 0, 2 }, { 60, 62 } }, meta.retrySeedDocs());
            }
        }
    }

    /**
     * 8 logical docs: logical 0 passes (vector 0.0, child 0), logical 1-6 fail (vectors 1..6),
     * logical 7 passes (vector 7.0, child 14). With k=2, numCands=3, and scale=1.5, the delegate
     * collects the 3 closest children {0,2,4}; only child 0 passes. Retry finds no passer, so the
     * query falls through to the bare inner query which returns children {0,14}.
     */
    public void testFallThroughToInnerQueryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                boolean passes = (i == 0 || i == 7);
                indexDocBlock(writer, "vector", (float) i, passes ? "pass" : "fail");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 2;
                int numCands = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 14 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.25f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0, 2, 4 }, meta.retryExcludedDocs());
                assertArrayEquals(new int[][] { { 0 } }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
            }
        }
    }

    /**
     * 10 logical docs: 6 fail (vectors 0..5), 4 pass (vectors 10..13, children 12/14/16/18). With
     * numCands=4, the delegate collects the 4 closest children {0,2,4,6}, all failing. Post-filter
     * returns null, falling through to the bare inner query which returns children {12,14,16,18}.
     */
    public void testFallsThroughToBareInnerWhenPostFilterYieldsNoMatches() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 6; i++) {
                indexDocBlock(writer, "vector", (float) i, "fail");
            }
            for (int i = 0; i < 4; i++) {
                indexDocBlock(writer, "vector", 10f + i, "pass");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                int numCands = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 12, 14, 16, 18 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    /**
     * 10 logical docs, selectivity=0.5 (even-vector children pass), threshold=0.9 &rarr; post-filter
     * gated off; outer rewrites straight to the bare inner query returning children {0,4,8,12}.
     */
    public void testSkipsPostFilterWhenSelectivityBelowThreshold() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 10; i++) {
                indexDocBlock(writer, "vector", (float) i, i % 2 == 0 ? "pass" : "fail");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0.9f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 4, 8, 12 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    /**
     * 20 logical docs, selectivity=0.7 (14 pass, 6 fail), arranged so the kNN region around the
     * query is negatively correlated to the filter. Early-exit fires: k=6 &ge; EARLY_EXIT_MIN_K and
     * the first-pass passer count &lt; (6*0.7)/2 = 2.1. The bare inner returns children
     * {6,8,16,18,20,22}.
     */
    public void testEarlyExitBailsOnNegativelyCorrelatedFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                indexDocBlock(writer, "vector", (float) i, "fail");
            }
            for (int i = 3; i < 5; i++) {
                indexDocBlock(writer, "vector", (float) i, "pass");
            }
            for (int i = 0; i < 3; i++) {
                indexDocBlock(writer, "vector", 50f + i, "fail");
            }
            for (int i = 0; i < 12; i++) {
                indexDocBlock(writer, "vector", 80f + i, "pass");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 6;
                int numCands = 7;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", parents, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 6, 8, 16, 18, 20, 22 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.7f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    public void testMatchNoDocsFilterShortCircuits() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                indexDocBlock(writer, "vector", (float) i, "pass");
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query filter = MatchNoDocsQuery.INSTANCE;
                BitSetProducer parents = parentFilter(reader);
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, filter, 1.5f, parents);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, filter, k, "vector", parents, 0f);

                Query rewritten = pfq.rewrite(searcher);
                assertSame(MatchNoDocsQuery.INSTANCE, rewritten);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    private static void assertDocsByScoreDescending(ScoreDoc[] actual, int[] expectedDocs) {
        assertEquals("doc count mismatch", expectedDocs.length, actual.length);
        for (int i = 0; i < expectedDocs.length; i++) {
            assertEquals("doc at rank " + i, expectedDocs[i], actual[i].doc);
            if (i > 0) {
                assertTrue(
                    "scores must be descending: rank " + (i - 1) + "=" + actual[i - 1].score + ", rank " + i + "=" + actual[i].score,
                    actual[i - 1].score >= actual[i].score
                );
            }
        }
    }
}
