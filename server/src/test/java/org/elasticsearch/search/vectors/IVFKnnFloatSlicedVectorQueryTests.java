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
import org.apache.lucene.document.KnnFloatVectorField;
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
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;

public class IVFKnnFloatSlicedVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase {

    private static final String SLICE_FIELD = "_slice";
    private int numSlices;
    private BytesRef querySlice;

    @Override
    IVFKnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatSlicedVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver(), SLICE_FIELD, querySlice);
    }

    @Override
    IVFKnnFloatVectorQuery getStableKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Before
    public void setUpIVFKnnFloatSlicedVectorQuery() throws Exception {
        format = new ESNextDiskBBQVectorsFormat(128, 4, SLICE_FIELD);
        // only one slice so it behaves as a normal index
        this.numSlices = 1;
        querySlice = new BytesRef("" + 0);
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomNormalizedFloatVector(dim);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10);
            assertEquals("IVFKnnFloatSlicedVectorQuery:field[0.0,...][10][" + SLICE_FIELD + "=[0]]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10, filter);
            assertEquals("IVFKnnFloatSlicedVectorQuery:field[0.0,...][10][" + SLICE_FIELD + "=[0]][id:text]", query.toString("ignored"));
        }
    }

    @Override
    protected Document getDocumentToIndex() {
        Document doc = new Document();
        doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("" + random().nextInt(numSlices))));
        return doc;
    }

    @Override
    protected void decorateIWC(IndexWriterConfig indexWriterConfig) {
        indexWriterConfig.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
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

    private void doTestSlices(BooleanSupplier supplier, boolean applyFilter) throws IOException {
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
                boolean hasVector = supplier.getAsBoolean();
                if (hasVector) {
                    docsPerSlice[slice]++;
                    if (filterMatch) {
                        docsPerSliceFiltered[slice]++;
                    }
                    doc.add(new KnnFloatVectorField("vector", randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
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
            float[] vector = randomVector(dimensions);
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

                        TopDocs topDocs = getTopDocs(searcher, expectedDocs, vector, filterQuery, slice);
                        assertTopDocs(applyFilter, expectedDocs, topDocs, reader, filterField, filterValue, slice);
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
                        TopDocs topDocs = getTopDocs(searcher, expectedDocs, vector, filterQuery, querySlices);
                        assertTopDocs(applyFilter, expectedDocs, topDocs, reader, filterField, filterValue, querySlices);
                    }
                    {
                        // all slices
                        int expectedDocs = 0;
                        for (int j = 0; j < numSlices; j++) {
                            expectedDocs += applyFilter ? docsPerSliceFiltered[j] : docsPerSlice[j];
                        }
                        TopDocs topDocs = getTopDocs(searcher, expectedDocs, vector, filterQuery);
                        assertEquals(expectedDocs, topDocs.scoreDocs.length);
                    }
                    // invalid slice
                    TopDocs topDocs = getTopDocs(searcher, 0, vector, filterQuery, -1);
                    assertEquals(0, topDocs.scoreDocs.length);
                }
            }
        }
    }

    private TopDocs getTopDocs(IndexSearcher searcher, int expectedDocs, float[] vector, Query filterQuery, int... slices)
        throws IOException {
        BytesRef[] sliceRef;
        int k;
        if (slices != null) {
            sliceRef = new BytesRef[slices.length];
            for (int i = 0; i < slices.length; i++) {
                sliceRef[i] = new BytesRef("" + slices[i]);
            }
            k = 2 * Math.max(1, expectedDocs);
        } else {
            sliceRef = null;
            k = expectedDocs;
        }
        Query kvq = new IVFKnnFloatSlicedVectorQuery("vector", vector, k, k, filterQuery, 1.0f, testResolver(), SLICE_FIELD, sliceRef);
        return searcher.search(kvq, k);
    }

    private static void assertTopDocs(
        boolean applyFilter,
        int expectedDocs,
        TopDocs topDocs,
        IndexReader reader,
        String filterField,
        String filterValue,
        int... slices
    ) throws IOException {
        assertEquals(expectedDocs, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
            int docSlice = Integer.parseInt(document.getField(SLICE_FIELD).binaryValue().utf8ToString());
            assertTrue(Arrays.stream(slices).anyMatch(s -> s == docSlice));
            if (applyFilter) {
                assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
            }
        }
    }
}
