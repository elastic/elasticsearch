/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES93HnswVectorsFormatTests extends BaseHnswVectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES93HnswVectorsFormat(
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            DenseVectorFieldMapper.ElementType.FLOAT,
            DEFAULT_NUM_MERGE_WORKER,
            null,
            random().nextInt(1, 20),
            false
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES93HnswVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            DEFAULT_NUM_MERGE_WORKER,
            null,
            random().nextInt(1, 20),
            false
        );
    }

    // copied from lucene upstream. Once this change
    // (https://github.com/apache/lucene/pull/16275) is added
    // to the library upstream, this method can be removed so we
    // use the one from the parent
    @Override
    public void testRandomWithUpdatesAndGraph() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        String fieldName = "field";
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, iwc)) {
            int numDoc = atLeast(100);
            int dimension = atLeast(10);
            if (dimension % 2 != 0) {
                dimension++;
            }
            float[][] id2value = new float[numDoc][];
            for (int i = 0; i < numDoc; i++) {
                int id = random().nextInt(numDoc);
                float[] value;
                if (random().nextInt(7) != 3) {
                    // usually index a vector value for a doc
                    value = randomNormalizedVector(dimension);
                } else {
                    value = null;
                }
                id2value[id] = value;
                add(iw, fieldName, id, value, VectorSimilarityFunction.EUCLIDEAN);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    Bits liveDocs = ctx.reader().getLiveDocs();
                    FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
                    if (vectorValues == null) {
                        continue;
                    }
                    StoredFields storedFields = ctx.reader().storedFields();
                    int docId;
                    int numLiveDocsWithVectors = 0;
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    while (true) {
                        if (!((docId = iterator.nextDoc()) != NO_MORE_DOCS)) break;
                        float[] v = vectorValues.vectorValue(iterator.index());
                        assertEquals(dimension, v.length);
                        String idString = storedFields.document(docId).getField("id").stringValue();
                        int id = Integer.parseInt(idString);
                        if (liveDocs == null || liveDocs.get(docId)) {
                            assertArrayEquals(
                                "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                                id2value[id],
                                v,
                                0
                            );
                            numLiveDocsWithVectors++;
                        } else {
                            if (id2value[id] != null) {
                                assertFalse(Arrays.equals(id2value[id], v));
                            }
                        }
                    }

                    if (numLiveDocsWithVectors == 0) {
                        continue;
                    }

                    // assert that searchNearestVectors returns the expected number of documents,
                    // in descending score order
                    int size = ctx.reader().getFloatVectorValues(fieldName).size();
                    int k = random().nextInt(size / 50 + 1) + 1;
                    if (k > numLiveDocsWithVectors) {
                        k = numLiveDocsWithVectors;
                    }
                    TopDocs results = ctx.reader()
                        .searchNearestVectors(
                            fieldName,
                            randomNormalizedVector(dimension),
                            k,
                            AcceptDocs.fromLiveDocs(liveDocs, ctx.reader().maxDoc()),
                            Integer.MAX_VALUE
                        );
                    assertEquals(Math.min(k, size), results.scoreDocs.length);
                    for (int i = 0; i < k - 1; i++) {
                        assertTrue(results.scoreDocs[i].score >= results.scoreDocs[i + 1].score);
                    }
                    assertOffHeapByteSize(ctx.reader(), fieldName);
                }
            }
        }
    }

    private void add(IndexWriter iw, String field, int id, float[] vector, VectorSimilarityFunction similarityFunction) throws IOException {
        Document doc = new Document();
        if (vector != null) {
            doc.add(new KnnFloatVectorField(field, vector, similarityFunction));
        }
        doc.add(new NumericDocValuesField("sortkey", random().nextLong(100)));
        String idString = Integer.toString(id);
        doc.add(new StringField("id", idString, Field.Store.YES));
        Term idTerm = new Term("id", idString);
        iw.updateDocument(idTerm, doc);
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES93HnswVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            numMergeWorkers,
            service,
            random().nextInt(1, 20),
            false
        );
    }

    protected KnnVectorsFormat createFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        ExecutorService service,
        int hnswGraphThreshold
    ) {
        return new ES93HnswVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            numMergeWorkers,
            service,
            hnswGraphThreshold,
            false
        );
    }

    public void testDefaultHnswGraphThreshold() {
        KnnVectorsFormat format = new ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT);
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD)));
    }

    public void testHnswGraphThresholdWithCustomValue() {
        int customThreshold = random().nextInt(1, 1001);
        KnnVectorsFormat format = createFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_NUM_MERGE_WORKER, null, customThreshold);
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + customThreshold)));
    }

    public void testHnswGraphThresholdWithZeroValue() {
        // When threshold is 0, hnswGraphThreshold is omitted from toString (always build graph)
        KnnVectorsFormat format = createFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_NUM_MERGE_WORKER, null, 0);
        assertThat(format.toString().contains("hnswGraphThreshold"), is(false));
    }

    public void testHnswGraphThresholdWithNegativeValueFallsBackToDefault() {
        KnnVectorsFormat format = createFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_NUM_MERGE_WORKER, null, -1);
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD)));
    }

    public void testToString() {
        int hnswGraphThreshold = random().nextInt(1, 1001);
        String expected =
            "ES93HnswVectorsFormat(name=ES93HnswVectorsFormat, maxConn=10, beamWidth=20, hnswGraphThreshold=%s, flatVectorFormat=%s)";
        expected = format(
            Locale.ROOT,
            expected,
            hnswGraphThreshold,
            "ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format=%s, useDirectIO=false, onDiskMerge=false)"
        );
        expected = format(Locale.ROOT, expected, "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer=%s)");
        expected = format(Locale.ROOT, expected, "ES93GenericFlatVectorScorer(delegate=%s)");
        String defaultScorer = format(Locale.ROOT, expected, "ESDefaultFlatVectorScorer(delegate=DefaultFlatVectorScorer())");
        String memSegScorer = format(Locale.ROOT, expected, "ESDefaultFlatVectorScorer(delegate=Lucene99MemorySegmentFlatVectorsScorer())");
        String nativeScorer = format(Locale.ROOT, expected, "PanamaFlatVectorScorer()");

        KnnVectorsFormat format = createFormat(10, 20, 1, null, hnswGraphThreshold);
        assertThat(format, hasToString(is(oneOf(defaultScorer, memSegScorer, nativeScorer))));
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        // Use threshold=0 to ensure HNSW graph is always built
        var format = new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 1, null, 0, false);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSize(
                dir,
                config,
                vector,
                allOf(aMapWithSize(2), hasEntry("vec", (long) vector.length * Float.BYTES), hasEntry("vex", 1L))
            );
        }
    }
}
