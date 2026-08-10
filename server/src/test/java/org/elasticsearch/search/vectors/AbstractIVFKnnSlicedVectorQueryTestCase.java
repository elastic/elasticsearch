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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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

    /**
     * Tests that querying multiple slices at once returns results from all requested slices.
     */
    public void testMultiSlice() throws IOException {
        int dimensions = random().nextInt(12, 128);
        int numDocs = random().nextInt(200, 2000);
        int numSlices = random().nextInt(3, 8);
        int[] docsPerSlice = new int[numSlices];
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("" + slice)));
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
        iwc.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("" + slice)));
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
        iwc.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("0")));
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
        iwc.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("" + slice)));
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
}
