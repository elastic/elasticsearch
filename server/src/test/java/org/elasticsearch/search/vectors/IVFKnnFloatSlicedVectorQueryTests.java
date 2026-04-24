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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static org.hamcrest.Matchers.equalTo;

public class IVFKnnFloatSlicedVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase {

    private static final String SLICE_FIELD = "_slice";
    private int numSlices;
    private BytesRef querySlice;

    @Override
    IVFKnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatSlicedVectorQuery(
            field,
            query,
            k,
            k,
            queryFilter,
            visitRatio,
            random().nextBoolean(),
            SLICE_FIELD,
            querySlice
        );
    }

    @Override
    IVFKnnFloatVectorQuery getStableKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatVectorQuery(field, query, k, k, queryFilter, visitRatio, random().nextBoolean());
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = new ESNextDiskBBQVectorsFormat(128, 4, SLICE_FIELD);
        // only one slice so it behaves as a normal index
        this.numSlices = 1;
        querySlice = new BytesRef("" + 0);
    }

    @Override
    float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = randomFloat();
        }
        VectorUtil.l2normalize(vector);
        return vector;
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
            assertEquals("IVFKnnFloatSlicedVectorQuery:field[0.0,...][10][" + SLICE_FIELD + "=0]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10, filter);
            assertEquals("IVFKnnFloatSlicedVectorQuery:field[0.0,...][10][" + SLICE_FIELD + "=0][id:text]", query.toString("ignored"));
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
        doTestSlices(() -> true);
    }

    public void testSlicesSparse() throws IOException {
        if (rarely()) {
            doTestSlices(() -> random().nextInt(1000) == 0);
        } else {
            int bound = random().nextInt(2, 50);
            doTestSlices(() -> random().nextInt(bound) == 0);
        }
    }

    private void doTestSlices(BooleanSupplier supplier) throws IOException {
        // TODO: add test with filters
        int dimensions = random().nextInt(12, 500);
        int numDocs = random().nextInt(100, 10_000);
        int[] docsPerSlice = new int[numSlices];
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(SLICE_FIELD, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(numSlices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(SLICE_FIELD, new BytesRef("" + slice)));
                if (supplier.getAsBoolean()) {
                    docsPerSlice[slice]++;
                    doc.add(new KnnFloatVectorField("vector", randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                }
                doc.add(new StoredField(SLICE_FIELD, new BytesRef("" + slice)));
                w.addDocument(doc);
            }
            w.commit();
            if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            float[] vector = randomVector(dimensions);
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = newSearcher(reader);
                for (int slice = 0; slice < numSlices; slice++) {
                    int k = 2 * Math.max(1, docsPerSlice[slice]);
                    Query kvq = new IVFKnnFloatSlicedVectorQuery(
                        "vector",
                        vector,
                        k,
                        k,
                        null,
                        1.0f,
                        random().nextBoolean(),
                        SLICE_FIELD,
                        new BytesRef("" + slice)
                    );
                    TopDocs topDocs = searcher.search(kvq, k);
                    assertEquals(docsPerSlice[slice], topDocs.scoreDocs.length);
                    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                        Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
                        assertThat(document.getField(SLICE_FIELD).binaryValue().utf8ToString(), equalTo("" + slice));
                    }
                }
                Query kvq = new IVFKnnFloatSlicedVectorQuery(
                    "vector",
                    vector,
                    3,
                    3,
                    null,
                    1.0f,
                    random().nextBoolean(),
                    SLICE_FIELD,
                    new BytesRef("invalid")
                );
                TopDocs topDocs = searcher.search(kvq, 3);
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }
}
