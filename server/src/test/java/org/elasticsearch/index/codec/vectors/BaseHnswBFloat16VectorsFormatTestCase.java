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
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class BaseHnswBFloat16VectorsFormatTestCase extends BaseBFloat16KnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    protected abstract KnnVectorsFormat createFormat();

    protected abstract KnnVectorsFormat createFormat(int maxConn, int beamWidth);

    protected abstract KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service);

    private KnnVectorsFormat format;

    @Override
    public void setUp() throws Exception {
        format = createFormat();
        super.setUp();
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
    }

    public void testLimits() {
        expectThrows(IllegalArgumentException.class, () -> createFormat(-1, 20));
        expectThrows(IllegalArgumentException.class, () -> createFormat(0, 20));
        expectThrows(IllegalArgumentException.class, () -> createFormat(20, 0));
        expectThrows(IllegalArgumentException.class, () -> createFormat(20, -1));
        expectThrows(IllegalArgumentException.class, () -> createFormat(512 + 1, 20));
        expectThrows(IllegalArgumentException.class, () -> createFormat(20, 3201));
        expectThrows(IllegalArgumentException.class, () -> createFormat(20, 100, 1, new SameThreadExecutorService()));
    }

    public void testSingleVectorCase() throws Exception {
        float[] vector = randomVector(random().nextInt(12, 500));
        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                if (similarityFunction == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(vector);
                }
                doc.add(new KnnFloatVectorField("f", vector, similarityFunction));
                w.addDocument(doc);
                w.commit();
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues("f");
                    KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
                    assertThat(vectorValues.size(), equalTo(1));
                    while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
                        assertArrayEquals(vector, vectorValues.vectorValue(docIndexIterator.index()), calculateDelta(vector));
                    }
                    float[] randomVector = randomVector(vector.length);
                    if (similarityFunction == VectorSimilarityFunction.COSINE) {
                        VectorUtil.l2normalize(randomVector);
                    }
                    float trueScore = similarityFunction.compare(vector, randomVector);
                    TopDocs td = r.searchNearestVectors(
                        "f",
                        randomVector,
                        1,
                        AcceptDocs.fromLiveDocs(r.getLiveDocs(), r.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertEquals(1, td.totalHits.value());
                    assertThat(td.scoreDocs[0].score, greaterThanOrEqualTo(0f));
                    // When it's the only vector in a segment, the score should be very close to the true score
                    assertEquals(trueScore, td.scoreDocs[0].score, trueScore / 100);
                }
            }
        }
    }

    protected static void testSimpleOffHeapSize(
        Directory dir,
        IndexWriterConfig config,
        float[] vector,
        Matcher<? super Map<String, Long>> matchesMap
    ) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, config)) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
            w.addDocument(doc);
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader r = getOnlyLeafReader(reader);
                if (r instanceof CodecReader codecReader) {
                    KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader("f");
                    }
                    var fieldInfo = r.getFieldInfos().fieldInfo("f");
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
                    assertThat(offHeap, matchesMap);
                }
            }
        }
    }
}
