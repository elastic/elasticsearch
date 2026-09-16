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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Abstract test case for IVF KNN sliced vector queries. Provides shared test infrastructure
 * for both float and byte vector sliced query implementations.
 */
public abstract class AbstractIVFKnnSlicedVectorQueryTestCase extends LuceneTestCase {

    protected static final String SLICE_FIELD = "_slice";

    static {
        LogConfigurator.configureESLogging();
    }

    protected ESNextDiskBBQVectorsFormat format;

    @Before
    public void initFormat() throws Exception {
        format = new ESNextDiskBBQVectorsFormat(128, 4, SLICE_FIELD);
    }

    /**
     * Adds the two doc-values fields a sliced document carries: the slice field holding the encoded slice key (the
     * index sort), and the numeric slice hash with a skip index that sliced search uses to prune whole leaves.
     */
    protected static void addSliceFields(Document doc, String sliceField, String sliceValue) {
        doc.add(SortedDocValuesField.indexedField(sliceField, SliceIndexing.encodeSliceKey(sliceValue)));
        doc.add(SortedNumericDocValuesField.indexedField(SliceIndexing.SLICE_HASH_FIELD_NAME, SliceIndexing.sliceHash(sliceValue)));
    }

    /** The index sort every sliced index must use: slice field first, STRING, ascending, missing values last. */
    protected static Sort sliceIndexSort() {
        SortField sliceSort = new SortField(SLICE_FIELD, SortField.Type.STRING);
        sliceSort.setMissingValue(SortField.STRING_LAST);
        return new Sort(sliceSort);
    }

    /** Creates a vector field with a random vector of the given dimensions. */
    protected abstract Field createVectorField(String name, int dimensions);

    /**
     * Creates the appropriate sliced vector query using a fresh random query vector of the given dimensions.
     * Implementations should generate a random query vector internally.
     */
    protected abstract Query createSlicedQuery(
        String field,
        int dimensions,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        BytesRef... sliceIds
    );

    /**
     * Creates the appropriate sliced vector query for toString testing.
     * The query vector should have at least 2 elements and the first element should match {@link #firstQueryElement()}.
     */
    protected abstract Query createToStringQuery(String field, int k, int numCands, Query filter, float visitRatio, BytesRef... sliceIds);

    /** Returns the vector similarity function used by this test. */
    protected abstract VectorSimilarityFunction similarityFunction();

    /** Returns the query toString prefix, e.g. "IVFKnnFloatSlicedVectorQuery" or "IVFKnnByteSlicedVectorQuery". */
    protected abstract String queryToStringPrefix();

    /** Returns the first element of the query vector for toString verification, e.g. "0.0" for float or "0" for byte. */
    protected abstract Object firstQueryElement();

    protected TestIvfQueryConfigResolver testResolver() {
        return new TestIvfQueryConfigResolver(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, false, 1.0f);
    }

    public void testSlicesDense() throws IOException {
        doTestSlicesDense(false);
    }

    public void testSlicesDenseWithFilter() throws IOException {
        doTestSlicesDense(true);
    }

    public void testSlicesSparse() throws IOException {
        doTestSlicesSparse(false);
    }

    public void testSlicesSparseWithFilter() throws IOException {
        doTestSlicesSparse(true);
    }

    private void doTestSlicesSparse(boolean applyFilter) throws IOException {
        if (rarely()) {
            doTestSlices(() -> random().nextInt(1000) == 0, applyFilter);
        } else {
            int bound = random().nextInt(2, 50);
            doTestSlices(() -> random().nextInt(bound) == 0, applyFilter);
        }
    }

    private void doTestSlicesDense(boolean applyFilter) throws IOException {
        doTestSlices(() -> true, applyFilter);
    }

    public void testTrailingMissingSliceDocValues() throws IOException {
        final int dimensions = random().nextInt(12, 128);
        final int numSlices = random().nextInt(2, 8);
        final int docsPerSlice = random().nextInt(2, 20);
        final int routedDocs = numSlices * docsPerSlice;
        final int tombstones = random().nextInt(1, Math.max(2, numSlices));
        final IndexWriterConfig iwc = newIndexWriterConfig();
        final SortField sliceSort = new SortField(SLICE_FIELD, SortField.Type.STRING, false, SortField.STRING_LAST);
        iwc.setIndexSort(new Sort(sliceSort));
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        // Retain soft-deleted tombstones through merges, as Elasticsearch does. Otherwise a randomized merge policy
        // can expunge every tombstone before the reader opens and no leaf remains sparse.
        iwc.setMergePolicy(
            new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, () -> Queries.ALL_DOCS_INSTANCE, iwc.getMergePolicy())
        );
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // Keep segments small enough to exercise both single- and multi-segment readers. Tombstones are interleaved
        // with routed documents below, so at least one segment contains both kinds of documents.
        iwc.setMaxBufferedDocs(random().nextInt(3, Math.min(20, routedDocs + tombstones + 1)));

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, iwc)) {
            int tombstonesAdded = 0;
            final int tombstoneInterval = Math.max(1, routedDocs / (tombstones + 1));
            for (int i = 0; i < routedDocs; i++) {
                final int slice = i % numSlices;
                final String sliceValue = Integer.toString(slice);
                final Document document = new Document();
                addSliceFields(document, SLICE_FIELD, sliceValue);
                document.add(new StoredField(SLICE_FIELD, new BytesRef(sliceValue)));
                document.add(createVectorField("vector", dimensions));
                writer.addDocument(document);

                if (tombstonesAdded < tombstones && (i + 1) % tombstoneInterval == 0) {
                    final Document tombstone = new Document();
                    tombstone.add(new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1L));
                    writer.addDocument(tombstone);
                    tombstonesAdded++;
                }
            }
            if (random().nextBoolean()) {
                // Also cover a single merged segment; the retention merge policy keeps its tombstones as a trailing suffix.
                writer.forceMerge(1);
            }
            writer.commit();

            try (DirectoryReader reader = new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(writer), Lucene.SOFT_DELETES_FIELD)) {
                assertEquals(routedDocs, reader.numDocs());
                int sparseLeaves = 0;
                for (var context : reader.leaves()) {
                    final var leaf = context.reader();
                    assertSame(SortField.STRING_LAST, leaf.getMetaData().sort().getSort()[0].getMissingValue());
                    final var skipper = leaf.getDocValuesSkipper(SLICE_FIELD);
                    if (skipper != null && skipper.docCount() < leaf.maxDoc()) {
                        sparseLeaves++;
                    }
                }
                assertTrue("expected at least one leaf with trailing missing slice doc values", sparseLeaves > 0);

                final IndexSearcher searcher = new IndexSearcher(reader);
                final int targetSlice = random().nextInt(numSlices);
                final String targetSliceValue = Integer.toString(targetSlice);
                final Query oneSlice = createSlicedQuery(
                    "vector",
                    dimensions,
                    docsPerSlice,
                    docsPerSlice,
                    null,
                    1.0f,
                    new BytesRef(targetSliceValue)
                );
                final TopDocs oneSliceResults = searcher.search(oneSlice, docsPerSlice);
                assertEquals(docsPerSlice, oneSliceResults.scoreDocs.length);
                for (var scoreDoc : oneSliceResults.scoreDocs) {
                    final Document document = reader.storedFields().document(scoreDoc.doc);
                    assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo(targetSliceValue));
                }

                final Query allSlices = createSlicedQuery("vector", dimensions, routedDocs, routedDocs, null, 1.0f);
                final TopDocs allSliceResults = searcher.search(allSlices, routedDocs);
                assertEquals(routedDocs, allSliceResults.scoreDocs.length);
                for (var scoreDoc : allSliceResults.scoreDocs) {
                    final Document document = reader.storedFields().document(scoreDoc.doc);
                    final int slice = Integer.parseInt(document.getField(SLICE_FIELD).binaryValue().utf8ToString());
                    assertTrue(slice >= 0 && slice < numSlices);
                }
            }
        }
    }

    /**
     * Tests that querying multiple slices at once returns results from all requested slices.
     */
    public void testMultiSlice() throws IOException {
        int dimensions = random().nextInt(12, 128);
        int numDocs = random().nextInt(200, 2000);
        int numSlices = random().nextInt(3, 8);
        int[] docsPerSlice = new int[numSlices];
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                addSliceFields(doc, SLICE_FIELD, "" + slice);
                doc.add(createVectorField("vector", dimensions));
                doc.add(new StoredField(SLICE_FIELD, new BytesRef("" + slice)));
                docsPerSlice[slice]++;
                w.addDocument(doc);
            }
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Query two slices at once
                int sliceA = 0;
                int sliceB = Math.min(1, numSlices - 1);
                int expectedTotal = docsPerSlice[sliceA] + (sliceA != sliceB ? docsPerSlice[sliceB] : 0);
                int k = 2 * Math.max(1, expectedTotal);
                Query kvq = createSlicedQuery("vector", dimensions, k, k, null, 1.0f, new BytesRef("" + sliceA), new BytesRef("" + sliceB));
                TopDocs topDocs = searcher.search(kvq, k);
                assertEquals(expectedTotal, topDocs.scoreDocs.length);
                // Verify all results come from the requested slices
                for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                    Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
                    String sliceValue = document.getField(SLICE_FIELD).binaryValue().utf8ToString();
                    assertTrue(
                        "Expected slice " + sliceA + " or " + sliceB + " but got " + sliceValue,
                        sliceValue.equals("" + sliceA) || sliceValue.equals("" + sliceB)
                    );
                }
            }
        }
    }

    /**
     * Tests that querying with an empty sliceIds array searches all slices (returns all vectors).
     */
    public void testAllSlices() throws IOException {
        int dimensions = random().nextInt(12, 128);
        int numDocs = random().nextInt(200, 2000);
        int numSlices = random().nextInt(3, 8);
        int totalWithVector = 0;
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                addSliceFields(doc, SLICE_FIELD, "" + slice);
                doc.add(createVectorField("vector", dimensions));
                doc.add(new StoredField(SLICE_FIELD, new BytesRef("" + slice)));
                totalWithVector++;
                w.addDocument(doc);
            }
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                int k = 2 * Math.max(1, totalWithVector);
                // Empty sliceIds = search all slices
                Query kvq = createSlicedQuery("vector", dimensions, k, k, null, 1.0f);
                TopDocs topDocs = searcher.search(kvq, k);
                assertEquals(totalWithVector, topDocs.scoreDocs.length);
            }
        }
    }

    public void testToString() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            addSliceFields(doc, SLICE_FIELD, "0");
            doc.add(createVectorField("field", 2));
            w.addDocument(doc);
            w.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BytesRef querySlice = new BytesRef("0");
                Query query = createToStringQuery("field", 10, 10, null, 1.0f, querySlice);
                assertEquals(
                    queryToStringPrefix() + ":field[" + firstQueryElement() + ",...][10][" + SLICE_FIELD + "=[0]]",
                    query.toString("ignored")
                );

                // test with filter
                Query filter = new TermQuery(new Term("id", "text"));
                query = createToStringQuery("field", 10, 10, filter, 1.0f, querySlice);
                assertEquals(
                    queryToStringPrefix() + ":field[" + firstQueryElement() + ",...][10][" + SLICE_FIELD + "=[0]][id:text]",
                    query.toString("ignored")
                );
            }
        }
    }

    /**
     * Two distinct slices whose 32-bit hashes collide share a key prefix but remain separate, adjacent terms, so each is
     * still a contiguous doc range and a query for one never returns the other.
     */
    public void testHashCollidingSlicesStayDistinct() throws IOException {
        final String[] colliding = findHashCollidingSlices();
        assumeTrue("no 32-bit slice hash collision found within the draw budget", colliding != null);
        final String sliceA = colliding[0];
        final String sliceB = colliding[1];
        assertNotEquals(sliceA, sliceB);
        assertEquals(SliceIndexing.sliceHash(sliceA), SliceIndexing.sliceHash(sliceB));
        final String[] slices = new String[] { sliceA, sliceB, "unrelated-one", "unrelated-two" };
        final int dimensions = random().nextInt(12, 128);
        final int docsPerSlice = random().nextInt(3, 20);
        final IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < docsPerSlice; i++) {
                for (String slice : slices) {
                    final Document doc = new Document();
                    addSliceFields(doc, SLICE_FIELD, slice);
                    doc.add(new StoredField(SLICE_FIELD, new BytesRef(slice)));
                    doc.add(createVectorField("vector", dimensions));
                    w.addDocument(doc);
                }
            }
            w.commit();
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals(1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final SortedDocValues keys = leaf.getSortedDocValues(SLICE_FIELD);
                final int ordA = keys.lookupTerm(SliceIndexing.encodeSliceKey(sliceA));
                final int ordB = keys.lookupTerm(SliceIndexing.encodeSliceKey(sliceB));
                assertTrue(ordA >= 0);
                assertTrue(ordB >= 0);
                assertEquals("colliding slices must be adjacent terms", 1, Math.abs(ordA - ordB));
                int minA = Integer.MAX_VALUE, maxA = -1, minB = Integer.MAX_VALUE, maxB = -1;
                for (int doc = keys.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = keys.nextDoc()) {
                    if (keys.ordValue() == ordA) {
                        minA = Math.min(minA, doc);
                        maxA = Math.max(maxA, doc);
                    } else if (keys.ordValue() == ordB) {
                        minB = Math.min(minB, doc);
                        maxB = Math.max(maxB, doc);
                    }
                }
                assertEquals("slice A must be one contiguous range", docsPerSlice, maxA - minA + 1);
                assertEquals("slice B must be one contiguous range", docsPerSlice, maxB - minB + 1);
                assertTrue("colliding slices must occupy disjoint, adjacent ranges", maxA + 1 == minB || maxB + 1 == minA);

                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String slice : new String[] { sliceA, sliceB }) {
                    final int k = 2 * docsPerSlice;
                    final Query query = createSlicedQuery("vector", dimensions, k, k, null, 1.0f, new BytesRef(slice));
                    final TopDocs topDocs = searcher.search(query, k);
                    assertEquals(docsPerSlice, topDocs.scoreDocs.length);
                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        final Document document = reader.storedFields().document(scoreDoc.doc);
                        assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo(slice));
                    }
                }
            }
        }
    }

    /**
     * Draws random slice values until two share a hash. A 32-bit birthday collision is expected after ~2^16 draws, so
     * the budget below is generous; returns {@code null} if it is exhausted.
     */
    private static String[] findHashCollidingSlices() {
        final Map<Long, String> seen = new HashMap<>();
        for (int i = 0; i < (1 << 19); i++) {
            final String candidate = TestUtil.randomSimpleString(random(), 6, 12);
            final String previous = seen.putIfAbsent(SliceIndexing.sliceHash(candidate), candidate);
            if (previous != null && previous.equals(candidate) == false) {
                return new String[] { previous, candidate };
            }
        }
        return null;
    }

    /**
     * Each leaf's slice-hash range comes from skipper metadata; a leaf whose range excludes the queried slice is not
     * searched. This checks the layout that makes that possible and that results are unaffected. The strict proof that
     * the excluded leaf's slice field is never opened is a separate test.
     */
    public void testExcludedLeavesAreSkipped() throws IOException {
        final String sliceA = "a-" + TestUtil.randomSimpleString(random(), 3, 8);
        String sliceB;
        do {
            sliceB = "b-" + TestUtil.randomSimpleString(random(), 3, 8);
        } while (SliceIndexing.sliceHash(sliceB) == SliceIndexing.sliceHash(sliceA));
        final int dimensions = random().nextInt(12, 128);
        final int docsPerSlice = random().nextInt(3, 20);
        final IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // One leaf per slice: no merging, and no flush before each commit.
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        iwc.setRAMBufferSizeMB(256);

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (String slice : new String[] { sliceA, sliceB }) {
                for (int i = 0; i < docsPerSlice; i++) {
                    final Document doc = new Document();
                    addSliceFields(doc, SLICE_FIELD, slice);
                    doc.add(new StoredField(SLICE_FIELD, new BytesRef(slice)));
                    doc.add(createVectorField("vector", dimensions));
                    w.addDocument(doc);
                }
                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals(2, reader.leaves().size());
                final long hashA = SliceIndexing.sliceHash(sliceA);
                int leavesContainingA = 0;
                for (LeafReaderContext ctx : reader.leaves()) {
                    final DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(SliceIndexing.SLICE_HASH_FIELD_NAME);
                    assertNotNull(skipper);
                    assertEquals("each leaf holds a single slice", skipper.minValue(), skipper.maxValue());
                    if (skipper.minValue() <= hashA && hashA <= skipper.maxValue()) {
                        leavesContainingA++;
                    }
                }
                assertEquals("exactly one leaf's hash range contains the queried slice", 1, leavesContainingA);

                final IndexSearcher searcher = new IndexSearcher(reader);
                final int k = 2 * docsPerSlice;
                final Query query = createSlicedQuery("vector", dimensions, k, k, null, 1.0f, new BytesRef(sliceA));
                final TopDocs topDocs = searcher.search(query, k);
                assertEquals(docsPerSlice, topDocs.scoreDocs.length);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    final Document document = reader.storedFields().document(scoreDoc.doc);
                    assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo(sliceA));
                }
            }
        }
    }

    /**
     * Strict read-proof counterpart of {@link #testExcludedLeavesAreSkipped}: proves that the excluded leaf's
     * slice field is never opened via {@code getSortedDocValues} or {@code getDocValuesSkipper}.
     */
    public void testExcludedLeafNeverOpensSliceField() throws IOException {
        final String sliceA = "a-" + TestUtil.randomSimpleString(random(), 3, 8);
        String sliceB;
        do {
            sliceB = "b-" + TestUtil.randomSimpleString(random(), 3, 8);
        } while (SliceIndexing.sliceHash(sliceB) == SliceIndexing.sliceHash(sliceA));
        final int dimensions = random().nextInt(12, 128);
        final int docsPerSlice = random().nextInt(3, 20);
        final IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        iwc.setRAMBufferSizeMB(256);

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (String slice : new String[] { sliceA, sliceB }) {
                for (int i = 0; i < docsPerSlice; i++) {
                    final Document doc = new Document();
                    addSliceFields(doc, SLICE_FIELD, slice);
                    doc.add(new StoredField(SLICE_FIELD, new BytesRef(slice)));
                    doc.add(createVectorField("vector", dimensions));
                    w.addDocument(doc);
                }
                w.commit();
            }
            try (DirectoryReader baseReader = DirectoryReader.open(w)) {
                assertEquals(2, baseReader.leaves().size());
                final FilterDirectoryReader wrappedReader = new FilterDirectoryReader(
                    baseReader,
                    new FilterDirectoryReader.SubReaderWrapper() {
                        @Override
                        public LeafReader wrap(LeafReader reader) {
                            return new RecordingLeafReader(reader);
                        }
                    }
                ) {
                    @Override
                    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                        return in;
                    }

                    @Override
                    public IndexReader.CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
                final IndexSearcher searcher = new IndexSearcher(wrappedReader);
                final int k = 2 * docsPerSlice;
                final Query query = createSlicedQuery("vector", dimensions, k, k, null, 1.0f, new BytesRef(sliceA));
                final TopDocs topDocs = searcher.search(query, k);
                assertEquals(docsPerSlice, topDocs.scoreDocs.length);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    final Document document = wrappedReader.storedFields().document(scoreDoc.doc);
                    assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo(sliceA));
                }
                assertEquals(2, wrappedReader.leaves().size());
                final long hashA = SliceIndexing.sliceHash(sliceA);
                int includedLeaves = 0;
                for (LeafReaderContext ctx : wrappedReader.leaves()) {
                    final RecordingLeafReader rlr = (RecordingLeafReader) ctx.reader();
                    // Read the hash skipper through the unwrapped inner reader to avoid polluting the recording.
                    final DocValuesSkipper skipper = rlr.getInner().getDocValuesSkipper(SliceIndexing.SLICE_HASH_FIELD_NAME);
                    assertNotNull(skipper);
                    final boolean included = skipper.minValue() <= hashA && hashA <= skipper.maxValue();
                    final Set<String> sdvOpened = new HashSet<>(rlr.sortedDocValuesOpened);
                    final Set<String> dvskOpened = new HashSet<>(rlr.docValuesSkipperOpened);
                    if (included) {
                        includedLeaves++;
                        assertTrue("included leaf must open the slice field's sorted doc values", sdvOpened.contains(SLICE_FIELD));
                    } else {
                        assertTrue(
                            "excluded leaf must open the slice hash field's skipper",
                            dvskOpened.contains(SliceIndexing.SLICE_HASH_FIELD_NAME)
                        );
                        assertFalse("excluded leaf must not open sorted doc values for slice field", sdvOpened.contains(SLICE_FIELD));
                        assertFalse("excluded leaf must not open doc values skipper for slice field", dvskOpened.contains(SLICE_FIELD));
                    }
                }
                assertEquals("exactly one leaf's hash range contains the queried slice", 1, includedLeaves);
            }
        }
    }

    private void doTestSlices(BooleanSupplier hasVectorSupplier, boolean applyFilter) throws IOException {
        int dimensions = random().nextInt(12, 500);
        int numDocs = random().nextInt(100, 10_000);
        int numSlices = random().nextInt(1, numDocs);
        int[] docsPerSlice = new int[numSlices];
        int[] docsPerSliceFiltered = new int[numSlices];
        int[] docSlices = new int[numDocs];
        boolean[] docHasVector = new boolean[numDocs];
        boolean[] docFilterMatch = new boolean[numDocs];
        String filterField = "_filter";
        String filterValue = "match";
        String filterMiss = "miss";
        String docIdField = "_doc_id";
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sliceIndexSort());
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                addSliceFields(doc, SLICE_FIELD, "" + slice);
                boolean filterMatch = random().nextBoolean();
                String filterText = filterMatch ? filterValue : filterMiss;
                doc.add(new StringField(filterField, filterText, Field.Store.NO));
                doc.add(new StoredField(filterField, new BytesRef(filterText)));
                doc.add(new StringField(docIdField, "doc_" + i, Field.Store.NO));
                boolean hasVector = hasVectorSupplier.getAsBoolean();
                if (hasVector) {
                    docsPerSlice[slice]++;
                    if (filterMatch) {
                        docsPerSliceFiltered[slice]++;
                    }
                    doc.add(createVectorField("vector", dimensions));
                }
                doc.add(new StoredField(SLICE_FIELD, new BytesRef("" + slice)));
                w.addDocument(doc);
                docSlices[i] = slice;
                docHasVector[i] = hasVector;
                docFilterMatch[i] = filterMatch;
            }
            w.commit();
            if (random().nextBoolean()) {
                int deleteCount = random().nextInt(0, Math.max(1, numDocs / 10));
                Set<Integer> docsToDelete = new HashSet<>();
                while (docsToDelete.size() < deleteCount) {
                    docsToDelete.add(random().nextInt(numDocs));
                }
                for (int docId : docsToDelete) {
                    if (docHasVector[docId]) {
                        docsPerSlice[docSlices[docId]]--;
                        if (docFilterMatch[docId]) {
                            docsPerSliceFiltered[docSlices[docId]]--;
                        }
                    }
                    w.deleteDocuments(new Term(docIdField, "doc_" + docId));
                }
                if (docsToDelete.isEmpty() == false) {
                    w.commit();
                }
            } else if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);
                Query filterQuery = null;
                if (applyFilter) {
                    filterQuery = new TermQuery(new Term(filterField, filterValue));
                }
                for (int iters = 0; iters < 2; iters++) {
                    // single slice
                    for (int slice = 0; slice < numSlices; slice++) {
                        int expectedDocs = applyFilter ? docsPerSliceFiltered[slice] : docsPerSlice[slice];
                        int k = 2 * Math.max(1, expectedDocs);
                        Query kvq = createSlicedQuery("vector", dimensions, k, k, filterQuery, 1.0f, new BytesRef("" + slice));
                        TopDocs topDocs = searcher.search(kvq, k);
                        assertEquals(expectedDocs, topDocs.scoreDocs.length);
                        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                            Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
                            assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo("" + slice));
                            if (applyFilter) {
                                assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
                            }
                        }
                    }
                    // multiple slices
                    for (int i = 0; i < 10; i++) {
                        int numQuerySlices = random().nextInt(numSlices) + 1;
                        int[] querySlices = new int[numQuerySlices];
                        int expectedDocs = 0;
                        int prevSlice = 0;
                        for (int j = 0; j < numQuerySlices; j++) {
                            querySlices[j] = random().nextInt(prevSlice, numSlices - numQuerySlices + j + 1);
                            expectedDocs += applyFilter ? docsPerSliceFiltered[querySlices[j]] : docsPerSlice[querySlices[j]];
                            prevSlice = querySlices[j] + 1;
                        }
                        Arrays.sort(querySlices);
                        BytesRef[] sliceRefs = new BytesRef[querySlices.length];
                        for (int j = 0; j < querySlices.length; j++) {
                            sliceRefs[j] = new BytesRef("" + querySlices[j]);
                        }
                        int k = 2 * Math.max(1, expectedDocs);
                        Query kvq = createSlicedQuery("vector", dimensions, k, k, filterQuery, 1.0f, sliceRefs);
                        TopDocs topDocs = searcher.search(kvq, k);
                        assertEquals(expectedDocs, topDocs.scoreDocs.length);
                        for (int idx = 0; idx < topDocs.scoreDocs.length; idx++) {
                            Document document = reader.storedFields().document(topDocs.scoreDocs[idx].doc);
                            int docSlice = Integer.parseInt(document.getField(SLICE_FIELD).binaryValue().utf8ToString());
                            assertTrue(Arrays.stream(querySlices).anyMatch(s -> s == docSlice));
                            if (applyFilter) {
                                assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
                            }
                        }
                    }
                    {
                        // all slices
                        int expectedDocs = 0;
                        for (int j = 0; j < numSlices; j++) {
                            expectedDocs += applyFilter ? docsPerSliceFiltered[j] : docsPerSlice[j];
                        }
                        int k = 2 * Math.max(1, expectedDocs);
                        Query kvq = createSlicedQuery("vector", dimensions, k, k, filterQuery, 1.0f);
                        TopDocs topDocs = searcher.search(kvq, k);
                        assertEquals(expectedDocs, topDocs.scoreDocs.length);
                    }
                    // invalid slice
                    Query kvq = createSlicedQuery("vector", dimensions, 3, 3, filterQuery, 1.0f, new BytesRef("invalid"));
                    TopDocs topDocs = searcher.search(kvq, 3);
                    assertEquals(0, topDocs.scoreDocs.length);
                }
            }
        }
    }

    private static final class RecordingLeafReader extends FilterLeafReader {
        final Set<String> sortedDocValuesOpened = new HashSet<>();
        final Set<String> docValuesSkipperOpened = new HashSet<>();

        RecordingLeafReader(LeafReader in) {
            super(in);
        }

        LeafReader getInner() {
            return in;
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            sortedDocValuesOpened.add(field);
            return super.getSortedDocValues(field);
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            docValuesSkipperOpened.add(field);
            return super.getDocValuesSkipper(field);
        }

        @Override
        public IndexReader.CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public IndexReader.CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
