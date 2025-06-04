/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.util.CombinedBitSet;
import org.elasticsearch.lucene.util.MatchAllBitSet;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.elasticsearch.search.internal.ContextIndexSearcher.intersectScorerAndBitSet;
import static org.elasticsearch.search.internal.ExitableDirectoryReader.ExitableLeafReader;
import static org.elasticsearch.search.internal.ExitableDirectoryReader.ExitablePointValues;
import static org.elasticsearch.search.internal.ExitableDirectoryReader.ExitableTerms;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ContextIndexSearcherTests extends ESTestCase {
    public void testIntersectScorerAndRoleBits() throws Exception {
        final Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE));

        Document document = new Document();
        document.add(new StringField("field1", "value1", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value2", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value3", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value4", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        iw.commit();
        iw.deleteDocuments(new Term("field1", "value3"));
        iw.close();
        DirectoryReader directoryReader = DirectoryReader.open(directory);
        IndexSearcher searcher = newSearcher(directoryReader);
        Weight weight = searcher.createWeight(
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("field2", "value1"))), 3f),
            ScoreMode.COMPLETE,
            1f
        );

        LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);

        CombinedBitSet bitSet = new CombinedBitSet(query(leaf, "field1", "value1"), leaf.reader().getLiveDocs());
        LeafCollector leafCollector = new LeafBucketCollector() {
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(0));
                assertThat(scorer.score(), equalTo(3f));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value2"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(1));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value3"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                fail("docId [" + doc + "] should have been deleted");
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value4"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(3));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        directoryReader.close();
        directory.close();
    }

    private int indexDocs(Directory directory) throws IOException {
        try (
            RandomIndexWriter iw = new RandomIndexWriter(
                random(),
                directory,
                new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            final int numDocs = randomIntBetween(500, 1000);
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new StringField("field", "value", Field.Store.NO));
                iw.addDocument(document);
                if (rarely()) {
                    iw.flush();
                }
            }
            return numDocs;
        }
    }

    /**
     * Check that knn queries rewrite parallelizes on the number of segments
     */
    public void testConcurrentRewrite() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(randomIntBetween(2, 5));
        try (Directory directory = newDirectory()) {
            indexDocs(directory);
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                ContextIndexSearcher searcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    randomBoolean(),
                    executor,
                    // create as many slices as possible
                    Integer.MAX_VALUE,
                    1
                );
                int numSegments = directoryReader.getContext().leaves().size();
                KnnFloatVectorQuery vectorQuery = new KnnFloatVectorQuery("float_vector", new float[] { 0, 0, 0 }, 10, null);
                vectorQuery.rewrite(searcher);
                // 1 task gets executed on the caller thread
                assertBusy(() -> assertEquals(numSegments - 1, executor.getCompletedTaskCount()));
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * Test that collection starts one task per slice, all offloaded to the separate executor, none executed in the caller thread
     */
    public void testConcurrentCollection() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(randomIntBetween(2, 5));
        try (Directory directory = newDirectory()) {
            int numDocs = indexDocs(directory);
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                ContextIndexSearcher searcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    randomBoolean(),
                    executor,
                    // create as many slices as possible
                    Integer.MAX_VALUE,
                    1
                );
                Integer totalHits = searcher.search(new MatchAllDocsQuery(), new TotalHitCountCollectorManager(searcher.getSlices()));
                assertEquals(numDocs, totalHits.intValue());
                int numExpectedTasks = ContextIndexSearcher.computeSlices(searcher.getIndexReader().leaves(), Integer.MAX_VALUE, 1).length;
                // check that each slice except for one that executes on the calling thread goes to the executor, no matter the queue size
                // or the number of slices
                assertBusy(() -> assertEquals(numExpectedTasks - 1, executor.getCompletedTaskCount()));
            }
        } finally {
            terminate(executor);
        }
    }

    public void testContextIndexSearcherSparseNoDeletions() throws IOException {
        doTestContextIndexSearcher(true, false);
    }

    public void testContextIndexSearcherDenseNoDeletions() throws IOException {
        doTestContextIndexSearcher(false, false);
    }

    public void testContextIndexSearcherSparseWithDeletions() throws IOException {
        doTestContextIndexSearcher(true, true);
    }

    public void testContextIndexSearcherDenseWithDeletions() throws IOException {
        doTestContextIndexSearcher(false, true);
    }

    public void doTestContextIndexSearcher(boolean sparse, boolean deletions) throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null));
        Document doc = new Document();
        StringField allowedField = new StringField("allowed", "yes", Field.Store.NO);
        doc.add(allowedField);
        StringField fooField = new StringField("foo", "bar", Field.Store.NO);
        doc.add(fooField);
        StringField deleteField = new StringField("delete", "no", Field.Store.NO);
        doc.add(deleteField);
        IntPoint pointField = new IntPoint("point", 1, 2);
        doc.add(pointField);
        w.addDocument(doc);
        if (deletions) {
            // add a document that matches foo:bar but will be deleted
            deleteField.setStringValue("yes");
            w.addDocument(doc);
            deleteField.setStringValue("no");
        }
        allowedField.setStringValue("no");
        w.addDocument(doc);
        if (sparse) {
            for (int i = 0; i < 1000; ++i) {
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        w.deleteDocuments(new Term("delete", "yes"));

        IndexSettings settings = IndexSettingsModule.newIndexSettings("_index", Settings.EMPTY);
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(w), new ShardId(settings.getIndex(), 0));
        BitsetFilterCache cache = new BitsetFilterCache(settings, BitsetFilterCache.Listener.NOOP);
        Query roleQuery = new TermQuery(new Term("allowed", "yes"));
        BitSet bitSet = cache.getBitSetProducer(roleQuery).getBitSet(reader.leaves().get(0));
        if (sparse) {
            assertThat(bitSet, instanceOf(SparseFixedBitSet.class));
        } else {
            assertThat(bitSet, anyOf(instanceOf(FixedBitSet.class), instanceOf(MatchAllBitSet.class)));
        }

        DocumentSubsetDirectoryReader filteredReader = new DocumentSubsetDirectoryReader(reader, cache, roleQuery);

        ContextIndexSearcher searcher = new ContextIndexSearcher(
            filteredReader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );

        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            assertThat(context.reader(), instanceOf(SequentialStoredFieldsLeafReader.class));
            SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
            assertNotNull(lf.getSequentialStoredFieldsReader());
        }
        // Assert wrapping
        assertEquals(ExitableDirectoryReader.class, searcher.getIndexReader().getClass());
        for (LeafReaderContext lrc : searcher.getIndexReader().leaves()) {
            assertEquals(ExitableLeafReader.class, lrc.reader().getClass());
            assertNotEquals(ExitableTerms.class, lrc.reader().terms("foo").getClass());
            assertNotEquals(ExitablePointValues.class, lrc.reader().getPointValues("point").getClass());
        }
        searcher.addQueryCancellation(() -> {});
        for (LeafReaderContext lrc : searcher.getIndexReader().leaves()) {
            assertEquals(ExitableTerms.class, lrc.reader().terms("foo").getClass());
            assertEquals(ExitablePointValues.class, lrc.reader().getPointValues("point").getClass());
        }

        // Searching a non-existing term will trigger a null scorer
        assertEquals(0, searcher.count(new TermQuery(new Term("non_existing_field", "non_existing_value"))));

        assertEquals(1, searcher.count(new TermQuery(new Term("foo", "bar"))));

        // make sure scorers are created only once, see #1725
        assertEquals(1, searcher.count(new CreateScorerOnceQuery(new MatchAllDocsQuery())));

        TopDocs topDocs = searcher.search(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "bar"))), 3f), 1);
        assertEquals(1, topDocs.totalHits.value());
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(3f, topDocs.scoreDocs[0].score, 0);

        IOUtils.close(reader, w, dir);
    }

    public void testComputeSlices() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        int numDocs = rarely() ? randomIntBetween(0, 1000) : randomIntBetween(1000, 25000);
        Document doc = new Document();
        for (int i = 0; i < numDocs; i++) {
            w.addDocument(doc);
        }
        DirectoryReader reader = w.getReader();
        List<LeafReaderContext> contexts = reader.leaves();
        int iter = randomIntBetween(16, 64);
        for (int i = 0; i < iter; i++) {
            int numThreads = randomIntBetween(1, 16);
            LeafSlice[] slices = ContextIndexSearcher.computeSlices(contexts, numThreads, 1);
            assertSlices(slices, numDocs, numThreads);
        }
        // expect exception for numThreads < 1
        int numThreads = randomIntBetween(-16, 0);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ContextIndexSearcher.computeSlices(contexts, numThreads, 1)
        );
        assertThat(ex.getMessage(), equalTo("maxSliceNum must be >= 1 (got " + numThreads + ")"));
        IOUtils.close(reader, w, dir);
    }

    private static void assertSlices(LeafSlice[] slices, int numDocs, int numThreads) {
        // checks that the number of slices is not bigger than the number of available threads
        // and each slice contains at least 10% of the data (which means the max number of slices is 10)
        int sumDocs = 0;
        assertThat(slices.length, lessThanOrEqualTo(numThreads));
        for (LeafSlice slice : slices) {
            int sliceDocs = slice.getMaxDocs();
            assertThat(sliceDocs, greaterThanOrEqualTo((int) (0.1 * numDocs)));
            sumDocs += sliceDocs;
        }
        assertThat(sumDocs, equalTo(numDocs));
    }

    public void testExitableTermsMinAndMax() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null));
        Document doc = new Document();
        StringField fooField = new StringField("foo", "bar", Field.Store.NO);
        doc.add(fooField);
        w.addDocument(doc);
        w.flush();

        DirectoryReader directoryReader = DirectoryReader.open(w);
        for (LeafReaderContext lfc : directoryReader.leaves()) {
            Terms terms = lfc.reader().terms("foo");
            FilterLeafReader.FilterTerms filterTerms = new ExitableTerms(terms, new ExitableDirectoryReader.QueryCancellation() {
                @Override
                public boolean isEnabled() {
                    return false;
                }

                @Override
                public void checkCancelled() {

                }
            }) {
                @Override
                public TermsEnum iterator() {
                    fail("Retrieving min and max should retrieve values from block tree instead of iterating");
                    return null;
                }
            };
            assertEquals("bar", filterTerms.getMin().utf8ToString());
            assertEquals("bar", filterTerms.getMax().utf8ToString());
        }
        w.close();
        directoryReader.close();
        dir.close();
    }

    public void testReduceIsCalledOnTimeout() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            ThreadPoolExecutor executor = null;
            try (DirectoryReader directoryReader = DirectoryReader.open(dir)) {
                if (randomBoolean()) {
                    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(randomIntBetween(2, 5));
                }
                ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    executor,
                    executor == null ? -1 : executor.getMaximumPoolSize(),
                    1
                );
                boolean[] called = new boolean[1];
                CollectorManager<Collector, Void> manager = new CollectorManager<>() {
                    @Override
                    public Collector newCollector() {
                        return BucketCollector.NO_OP_COLLECTOR;
                    }

                    @Override
                    public Void reduce(Collection<Collector> collectors) {
                        called[0] = true;
                        return null;
                    }
                };
                contextIndexSearcher.search(new TestQuery() {
                    @Override
                    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
                        if (randomBoolean()) {
                            contextIndexSearcher.throwTimeExceededException();
                        }
                        return super.rewrite(indexSearcher);
                    }

                    @Override
                    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                        if (randomBoolean()) {
                            contextIndexSearcher.throwTimeExceededException();
                        }
                        return new ConstantScoreWeight(this, boost) {
                            @Override
                            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                                contextIndexSearcher.throwTimeExceededException();
                                Scorer scorer = new ConstantScoreScorer(
                                    score(),
                                    scoreMode,
                                    DocIdSetIterator.all(context.reader().maxDoc())
                                );
                                return new DefaultScorerSupplier(scorer);
                            }

                            @Override
                            public boolean isCacheable(LeafReaderContext ctx) {
                                return false;
                            }
                        };
                    }
                }, manager);
                assertTrue(contextIndexSearcher.timeExceeded());
                assertThat(called[0], equalTo(true));
            } finally {
                if (executor != null) {
                    terminate(executor);
                }
            }
        }
    }

    public void testTimeoutOnRewriteStandalone() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            ThreadPoolExecutor executor = null;
            try (DirectoryReader directoryReader = DirectoryReader.open(dir)) {
                if (randomBoolean()) {
                    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(randomIntBetween(2, 5));
                }
                ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    executor,
                    executor == null ? -1 : executor.getMaximumPoolSize(),
                    1
                );
                TestQuery testQuery = new TestQuery() {
                    @Override
                    public Query rewrite(IndexSearcher indexSearcher) {
                        contextIndexSearcher.throwTimeExceededException();
                        assert false;
                        return null;
                    }
                };
                Query rewrite = contextIndexSearcher.rewrite(testQuery);
                assertThat(rewrite, instanceOf(MatchNoDocsQuery.class));
                assertEquals("MatchNoDocsQuery(\"rewrite timed out\")", rewrite.toString());
                assertTrue(contextIndexSearcher.timeExceeded());
            } finally {
                if (executor != null) {
                    terminate(executor);
                }
            }
        }
    }

    public void testTimeoutOnRewriteDuringSearch() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            ThreadPoolExecutor executor = null;
            try (DirectoryReader directoryReader = DirectoryReader.open(dir)) {
                if (randomBoolean()) {
                    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(randomIntBetween(2, 5));
                }
                ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    executor,
                    executor == null ? -1 : executor.getMaximumPoolSize(),
                    1
                );
                TestQuery testQuery = new TestQuery() {
                    @Override
                    public Query rewrite(IndexSearcher indexSearcher) {
                        contextIndexSearcher.throwTimeExceededException();
                        assert false;
                        return null;
                    }
                };
                Integer hitCount = contextIndexSearcher.search(
                    testQuery,
                    new TotalHitCountCollectorManager(contextIndexSearcher.getSlices())
                );
                assertEquals(0, hitCount.intValue());
                assertTrue(contextIndexSearcher.timeExceeded());
            } finally {
                if (executor != null) {
                    terminate(executor);
                }
            }
        }
    }

    private static class TestQuery extends Query {
        @Override
        public String toString(String field) {
            return "query";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object o) {
            return sameClassAs(o);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }

    private SparseFixedBitSet query(LeafReaderContext leaf, String field, String value) throws IOException {
        SparseFixedBitSet sparseFixedBitSet = new SparseFixedBitSet(leaf.reader().maxDoc());
        TermsEnum tenum = leaf.reader().terms(field).iterator();
        while (tenum.next().utf8ToString().equals(value) == false) {
        }
        PostingsEnum penum = tenum.postings(null);
        sparseFixedBitSet.or(penum);
        return sparseFixedBitSet;
    }

    public static class DocumentSubsetDirectoryReader extends FilterDirectoryReader {
        private final BitsetFilterCache bitsetFilterCache;
        private final Query roleQuery;

        public DocumentSubsetDirectoryReader(DirectoryReader in, BitsetFilterCache bitsetFilterCache, Query roleQuery) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new DocumentSubsetReader(reader, bitsetFilterCache, roleQuery);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                }
            });
            this.bitsetFilterCache = bitsetFilterCache;
            this.roleQuery = roleQuery;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static class DocumentSubsetReader extends SequentialStoredFieldsLeafReader {
        private final BitSet roleQueryBits;
        private final int numDocs;

        /**
         * <p>Construct a FilterLeafReader based on the specified base reader.
         * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
         *
         * @param in specified base reader.
         */
        DocumentSubsetReader(LeafReader in, BitsetFilterCache bitsetFilterCache, Query roleQuery) throws IOException {
            super(in);
            this.roleQueryBits = bitsetFilterCache.getBitSetProducer(roleQuery).getBitSet(in.getContext());
            this.numDocs = computeNumDocs(in, roleQueryBits);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // Not delegated since we change the live docs
            return null;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public Bits getLiveDocs() {
            final Bits actualLiveDocs = in.getLiveDocs();
            if (roleQueryBits == null) {
                return new Bits.MatchNoBits(in.maxDoc());
            } else if (actualLiveDocs == null) {
                return roleQueryBits;
            } else {
                // apply deletes when needed:
                return new CombinedBitSet(roleQueryBits, actualLiveDocs);
            }
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        private static int computeNumDocs(LeafReader reader, BitSet roleQueryBits) {
            final Bits liveDocs = reader.getLiveDocs();
            if (roleQueryBits == null) {
                return 0;
            } else if (liveDocs == null) {
                // slow
                return roleQueryBits.cardinality();
            } else {
                // very slow, but necessary in order to be correct
                int numDocs = 0;
                DocIdSetIterator it = new BitSetIterator(roleQueryBits, 0L); // we don't use the cost
                try {
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (liveDocs.get(doc)) {
                            numDocs++;
                        }
                    }
                    return numDocs;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class CreateScorerOnceWeight extends Weight {

        private final Weight weight;
        private final Set<Object> seenLeaves = Collections.newSetFromMap(new IdentityHashMap<>());

        CreateScorerOnceWeight(Weight weight) {
            super(weight.getQuery());
            this.weight = weight;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheHelper().getKey()));
            return weight.scorerSupplier(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    private static class CreateScorerOnceQuery extends Query {

        private final Query query;

        CreateScorerOnceQuery(Query query) {
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return query.toString(field);
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query queryRewritten = query.rewrite(searcher);
            if (query != queryRewritten) {
                return new CreateScorerOnceQuery(queryRewritten);
            }
            return super.rewrite(searcher);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
            return new CreateScorerOnceWeight(query.createWeight(searcher, scoreMode, boost));
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && query.equals(((CreateScorerOnceQuery) obj).query);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + query.hashCode();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }
}
