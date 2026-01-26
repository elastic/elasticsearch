/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public abstract class BaseBFloat16KnnVectorsFormatTestCase extends BaseKnnVectorsFormatTestCase {

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testEmptyByteVectorData() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    public void testMergingWithDifferentByteKnnFields() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    public void testByteVectorScorerIteration() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    public void testSortedIndexBytes() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    public void testMismatchedFields() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    public void testRandomBytes() throws Exception {
        throw new AssumptionViolatedException("No bytes");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testWriterRamEstimate() throws Exception {
        final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
        final Directory dir = newDirectory();
        Codec codec = Codec.getDefault();
        final SegmentInfo si = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
        final SegmentWriteState state = new SegmentWriteState(InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
        final KnnVectorsFormat format = codec.knnVectorsFormat();
        try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
            final long ramBytesUsed = writer.ramBytesUsed();
            int dim = random().nextInt(64) + 1;
            if (dim % 2 == 1) {
                ++dim;
            }
            int numDocs = atLeast(100);
            KnnFieldVectorsWriter<float[]> fieldWriter = (KnnFieldVectorsWriter<float[]>) writer.addField(
                new FieldInfo(
                    "fieldA",
                    0,
                    false,
                    false,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.NONE,
                    DocValuesSkipIndexType.NONE,
                    -1,
                    Map.of(),
                    0,
                    0,
                    0,
                    dim,
                    VectorEncoding.FLOAT32,
                    VectorSimilarityFunction.DOT_PRODUCT,
                    false,
                    false
                )
            );
            for (int i = 0; i < numDocs; i++) {
                fieldWriter.addValue(i, randomVector(dim));
            }
            final long ramBytesUsed2 = writer.ramBytesUsed();
            assertTrue(ramBytesUsed2 > ramBytesUsed);
            assertTrue(ramBytesUsed2 > (long) dim * numDocs * BFloat16.BYTES);
        }
        dir.close();
    }

    @Override
    public void testRandom() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        if (random().nextBoolean()) {
            iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
        }
        String fieldName = "field";
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, iwc)) {
            int numDoc = atLeast(100);
            int dimension = atLeast(10);
            if (dimension % 2 != 0) {
                dimension++;
            }
            float[] scratch = new float[dimension];
            int numValues = 0;
            float[][] values = new float[numDoc][];
            for (int i = 0; i < numDoc; i++) {
                if (random().nextInt(7) != 3) {
                    // usually index a vector value for a doc
                    values[i] = randomNormalizedVector(dimension);
                    ++numValues;
                }
                if (random().nextBoolean() && values[i] != null) {
                    // sometimes use a shared scratch array
                    System.arraycopy(values[i], 0, scratch, 0, scratch.length);
                    add(iw, fieldName, i, scratch, similarityFunction);
                } else {
                    add(iw, fieldName, i, values[i], similarityFunction);
                }
                if (random().nextInt(10) == 2) {
                    // sometimes delete a random document
                    int idToDelete = random().nextInt(i + 1);
                    iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
                    // and remember that it was deleted
                    if (values[idToDelete] != null) {
                        values[idToDelete] = null;
                        --numValues;
                    }
                }
                if (random().nextInt(10) == 3) {
                    iw.commit();
                }
            }
            int numDeletes = 0;
            try (IndexReader reader = DirectoryReader.open(iw)) {
                int valueCount = 0, totalSize = 0;
                for (LeafReaderContext ctx : reader.leaves()) {
                    FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
                    if (vectorValues == null) {
                        continue;
                    }
                    totalSize += vectorValues.size();
                    StoredFields storedFields = ctx.reader().storedFields();
                    int docId;
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    while (true) {
                        if (((docId = iterator.nextDoc()) != NO_MORE_DOCS) == false) break;
                        float[] v = vectorValues.vectorValue(iterator.index());
                        assertEquals(dimension, v.length);
                        String idString = storedFields.document(docId).getField("id").stringValue();
                        int id = Integer.parseInt(idString);
                        if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(docId)) {
                            assertBFloat16Similar(idString + " " + docId, values[id], v);
                            ++valueCount;
                        } else {
                            ++numDeletes;
                            assertNull(values[id]);
                        }
                    }
                }
                assertEquals(numValues, valueCount);
                assertEquals(numValues, totalSize - numDeletes);
            }
        }
    }

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
                        if (((docId = iterator.nextDoc()) != NO_MORE_DOCS) == false) break;
                        float[] v = vectorValues.vectorValue(iterator.index());
                        assertEquals(dimension, v.length);
                        String idString = storedFields.document(docId).getField("id").stringValue();
                        int id = Integer.parseInt(idString);
                        if (liveDocs == null || liveDocs.get(docId)) {
                            assertBFloat16Similar(
                                "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                                id2value[id],
                                v
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
                    int k = random().nextInt(size / 10 + 1) + 1;
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

    @Override
    public void testSparseVectors() throws Exception {
        int numDocs = atLeast(1000);
        int numFields = TestUtil.nextInt(random(), 1, 10);
        int[] fieldDocCounts = new int[numFields];
        double[] fieldTotals = new double[numFields];
        int[] fieldDims = new int[numFields];
        VectorSimilarityFunction[] fieldSimilarityFunctions = new VectorSimilarityFunction[numFields];
        VectorEncoding[] fieldVectorEncodings = new VectorEncoding[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldDims[i] = random().nextInt(20) + 1;
            if (fieldDims[i] % 2 != 0) {
                fieldDims[i]++;
            }
            fieldSimilarityFunctions[i] = randomSimilarity();
            fieldVectorEncodings[i] = randomVectorEncoding();
        }
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                for (int field = 0; field < numFields; field++) {
                    String fieldName = "int" + field;
                    if (random().nextInt(100) == 17) {
                        switch (fieldVectorEncodings[field]) {
                            case BYTE -> {
                                byte[] b = randomVector8(fieldDims[field]);
                                doc.add(new KnnByteVectorField(fieldName, b, fieldSimilarityFunctions[field]));
                                fieldTotals[field] += b[0];
                            }
                            case FLOAT32 -> {
                                float[] v = randomNormalizedVector(fieldDims[field]);
                                doc.add(new KnnFloatVectorField(fieldName, v, fieldSimilarityFunctions[field]));
                                fieldTotals[field] += v[0];
                            }
                        }
                        fieldDocCounts[field]++;
                    }
                }
                w.addDocument(doc);
            }
            try (IndexReader r = w.getReader()) {
                for (int field = 0; field < numFields; field++) {
                    int docCount = 0;
                    double checksum = 0;
                    String fieldName = "int" + field;
                    switch (fieldVectorEncodings[field]) {
                        case BYTE -> {
                            for (LeafReaderContext ctx : r.leaves()) {
                                ByteVectorValues byteVectorValues = ctx.reader().getByteVectorValues(fieldName);
                                if (byteVectorValues != null) {
                                    docCount += byteVectorValues.size();
                                    KnnVectorValues.DocIndexIterator iterator = byteVectorValues.iterator();
                                    while (true) {
                                        if ((iterator.nextDoc() != NO_MORE_DOCS) == false) break;
                                        checksum += byteVectorValues.vectorValue(iterator.index())[0];
                                    }
                                }
                            }
                        }
                        case FLOAT32 -> {
                            for (LeafReaderContext ctx : r.leaves()) {
                                FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
                                if (vectorValues != null) {
                                    docCount += vectorValues.size();
                                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                                    while (true) {
                                        if ((iterator.nextDoc() != NO_MORE_DOCS) == false) break;
                                        checksum += vectorValues.vectorValue(iterator.index())[0];
                                    }
                                }
                            }
                        }
                    }
                    assertEquals(fieldDocCounts[field], docCount);
                    assertBFloat16Similar(fieldTotals[field], checksum);
                }
            }
        }
    }

    @Override
    public void testVectorValuesReportCorrectDocs() throws Exception {
        final int numDocs = atLeast(1000);
        int dim = random().nextInt(20) + 1;
        if (dim % 2 != 0) {
            dim++;
        }
        VectorEncoding vectorEncoding = randomVectorEncoding();
        VectorSimilarityFunction similarityFunction = randomSimilarity();

        double fieldValuesCheckSum = 0;
        int fieldDocCount = 0;
        long fieldSumDocIDs = 0;

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int docID = random().nextInt(numDocs);
                doc.add(new StoredField("id", docID));
                if (random().nextInt(4) == 3) {
                    switch (vectorEncoding) {
                        case BYTE -> {
                            byte[] b = randomVector8(dim);
                            fieldValuesCheckSum += b[0];
                            doc.add(new KnnByteVectorField("knn_vector", b, similarityFunction));
                        }
                        case FLOAT32 -> {
                            float[] v = randomNormalizedVector(dim);
                            fieldValuesCheckSum += v[0];
                            doc.add(new KnnFloatVectorField("knn_vector", v, similarityFunction));
                        }
                    }
                    fieldDocCount++;
                    fieldSumDocIDs += docID;
                }
                w.addDocument(doc);
            }

            if (random().nextBoolean()) {
                w.forceMerge(1);
            }

            try (IndexReader r = w.getReader()) {
                double checksum = 0;
                int docCount = 0;
                long sumDocIds = 0;
                long sumOrdToDocIds = 0;
                switch (vectorEncoding) {
                    case BYTE -> {
                        for (LeafReaderContext ctx : r.leaves()) {
                            ByteVectorValues byteVectorValues = ctx.reader().getByteVectorValues("knn_vector");
                            if (byteVectorValues != null) {
                                docCount += byteVectorValues.size();
                                StoredFields storedFields = ctx.reader().storedFields();
                                KnnVectorValues.DocIndexIterator iter = byteVectorValues.iterator();
                                for (iter.nextDoc(); iter.docID() != NO_MORE_DOCS; iter.nextDoc()) {
                                    int ord = iter.index();
                                    checksum += byteVectorValues.vectorValue(ord)[0];
                                    Document doc = storedFields.document(iter.docID(), Set.of("id"));
                                    sumDocIds += Integer.parseInt(doc.get("id"));
                                }
                                for (int ord = 0; ord < byteVectorValues.size(); ord++) {
                                    Document doc = storedFields.document(byteVectorValues.ordToDoc(ord), Set.of("id"));
                                    sumOrdToDocIds += Integer.parseInt(doc.get("id"));
                                }
                                assertOffHeapByteSize(ctx.reader(), "knn_vector");
                            }
                        }
                    }
                    case FLOAT32 -> {
                        for (LeafReaderContext ctx : r.leaves()) {
                            FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues("knn_vector");
                            if (vectorValues != null) {
                                docCount += vectorValues.size();
                                StoredFields storedFields = ctx.reader().storedFields();
                                KnnVectorValues.DocIndexIterator iter = vectorValues.iterator();
                                for (iter.nextDoc(); iter.docID() != NO_MORE_DOCS; iter.nextDoc()) {
                                    int ord = iter.index();
                                    checksum += vectorValues.vectorValue(ord)[0];
                                    Document doc = storedFields.document(iter.docID(), Set.of("id"));
                                    sumDocIds += Integer.parseInt(doc.get("id"));
                                }
                                for (int ord = 0; ord < vectorValues.size(); ord++) {
                                    Document doc = storedFields.document(vectorValues.ordToDoc(ord), Set.of("id"));
                                    sumOrdToDocIds += Integer.parseInt(doc.get("id"));
                                }
                                assertOffHeapByteSize(ctx.reader(), "knn_vector");
                            }
                        }
                    }
                }
                assertBFloat16Similar("encoding=" + vectorEncoding, fieldValuesCheckSum, checksum);
                assertEquals(fieldDocCount, docCount);
                assertEquals(fieldSumDocIDs, sumDocIds);
                assertEquals(fieldSumDocIDs, sumOrdToDocIds);
            }
        }
    }

    private static void add(IndexWriter iw, String field, int id, float[] vector, VectorSimilarityFunction similarityFunction)
        throws IOException {
        add(iw, field, id, random().nextInt(100), vector, similarityFunction);
    }

    private static void add(IndexWriter iw, String field, int id, int sortkey, float[] vector, VectorSimilarityFunction similarityFunction)
        throws IOException {
        Document doc = new Document();
        if (vector != null) {
            doc.add(new KnnFloatVectorField(field, vector, similarityFunction));
        }
        doc.add(new NumericDocValuesField("sortkey", sortkey));
        String idString = Integer.toString(id);
        doc.add(new StringField("id", idString, Field.Store.YES));
        Term idTerm = new Term("id", idString);
        iw.updateDocument(idTerm, doc);
    }

    /* BFloat16 values should be within 1% of expected value */

    protected static float calculateDelta(float[] values) {
        // use 1% of the max value for the delta
        float max = Float.MIN_VALUE;
        for (float v : values) {
            max = Math.max(max, v);
        }
        return max / 100;
    }

    private static void assertBFloat16Similar(double expected, double actual) {
        assertEquals(expected, actual, expected / 100);
    }

    private static void assertBFloat16Similar(String msg, double expected, double actual) {
        assertEquals(msg, expected, actual, expected / 100);
    }

    private static void assertBFloat16Similar(String msg, float[] expected, float[] actual) {
        assertArrayEquals(msg, expected, actual, calculateDelta(expected));
    }
}
