/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.BaseByteKnnVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.CalibrationUtils;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.ESAcceptDocs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for byte vector support with the ESNext IVF disk BBQ vectors format.
 */
public class ESNextDiskBBQByteVectorsFormatTests extends BaseByteKnnVectorsFormatTestCase {

    private KnnVectorsFormat format;

    @Override
    protected Codec getCodec() {
        if (format == null) {
            QuantEncoding encoding = QuantEncoding.values()[random().nextInt(QuantEncoding.values().length)];
            if (rarely()) {
                format = new ESNextDiskBBQVectorsFormat(
                    encoding,
                    random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER),
                    random().nextInt(8, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                    DenseVectorFieldMapper.ElementType.BYTE,
                    false,
                    null,
                    1,
                    false,
                    DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                    null
                );
            } else if (rarely()) {
                format = new ESNextDiskBBQVectorsFormat(
                    encoding,
                    random().nextInt(MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER),
                    random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                    DenseVectorFieldMapper.ElementType.BYTE,
                    false,
                    null,
                    1,
                    true,
                    random().nextInt(MIN_PRECONDITIONING_BLOCK_DIMS, MAX_PRECONDITIONING_BLOCK_DIMS),
                    null
                );
            } else {
                // run with low numbers to force many clusters with parents
                format = new ESNextDiskBBQVectorsFormat(
                    encoding,
                    random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER),
                    random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, 8),
                    DenseVectorFieldMapper.ElementType.BYTE,
                    false,
                    null,
                    1,
                    false,
                    DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                    null
                );
            }
        }
        return TestUtil.alwaysKnnVectorsFormat(format);
    }

    /**
     * A force merge of a byte field with auto-calibration enabled must sample the byte vectors (widened to float)
     * and calibrate, rather than fail reading them as float vectors.
     */
    public void testAutoCalibrationOnByteMerge() throws IOException {
        int dimensions = 32;
        int numDocs = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION + random().nextInt(500);
        VectorSimilarityFunction similarity = Arrays.stream(VectorSimilarityFunction.values())
            .filter(v -> v != VectorSimilarityFunction.COSINE)
            .toList()
            .get(random().nextInt(VectorSimilarityFunction.values().length - 1));
        IvfMergeConfigResolver calibrating = IvfAutoCalibration.mergeConfigResolver(
            MIN_VECTORS_PER_CLUSTER * 4,
            IvfAutoCalibrationProfile.QUALITY
        );
        AtomicInteger resolverCalls = new AtomicInteger();
        // wraps the real resolver to assert on the merge inputs it sees, which are not otherwise observable from the test
        IvfMergeConfigResolver verifying = (fieldInfo, mergeState, codecDefault) -> {
            resolverCalls.incrementAndGet();
            assertThat(fieldInfo.getVectorEncoding(), equalTo(VectorEncoding.BYTE));
            assertThat(CalibrationUtils.countMergedVectors(fieldInfo, mergeState), equalTo(numDocs));
            FloatVectorValues sampled = CalibrationUtils.buildSampled(fieldInfo, mergeState, numDocs);
            assertTrue(sampled.size() > 0);
            assertThat(sampled.dimension(), equalTo(dimensions));
            // a copy reads independently of the original, which reuses one scratch buffer across reads
            FloatVectorValues sampledCopy = sampled.copy();
            float[] live = sampled.vectorValue(0);
            float[] expected = live.clone();
            sampledCopy.vectorValue(1);
            assertArrayEquals(expected, live, 0f);
            for (int i = 0; i < sampled.size(); i++) {
                for (float v : sampled.vectorValue(i)) {
                    assertEquals("byte value widened exactly", v, (byte) v, 0.0);
                }
            }
            return calibrating.resolve(fieldInfo, mergeState, codecDefault);
        };
        ESNextDiskBBQVectorsFormat autoCalibrateFormat = new ESNextDiskBBQVectorsFormat(
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            MIN_VECTORS_PER_CLUSTER * 4,
            MIN_CENTROIDS_PER_PARENT_CLUSTER,
            DenseVectorFieldMapper.ElementType.BYTE,
            false,
            null,
            1,
            false,
            DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
            MIN_VECTORS_PER_CLUSTER * 4,
            null,
            IvfFlushConfigSource.empty(),
            verifying,
            false
        );
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(autoCalibrateFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new KnnByteVectorField("vector", randomByteVector(dimensions), similarity));
                w.addDocument(doc);
                if (i % 3_000 == 2_999) {
                    w.commit();
                }
            }
            w.commit();
            // a bounded force merge always recalibrates instead of reusing the inputs' metadata
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertThat(reader.leaves().size(), equalTo(1));
                assertThat(reader.numDocs(), equalTo(numDocs));
            }
        }
        assertThat(resolverCalls.get(), greaterThan(0));
    }

    /**
     * Tests byte vector indexing with sliced (partitioned) segments.
     * Verifies that byte-native clustering works correctly per-slice during both flush and merge.
     */
    public void testSlicedByteVectors() throws IOException {
        String sliceField = "_slice";
        QuantEncoding encoding = QuantEncoding.values()[random().nextInt(QuantEncoding.values().length)];
        int vectorPerCluster = random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER);
        ESNextDiskBBQVectorsFormat slicedFormat = new ESNextDiskBBQVectorsFormat(
            encoding,
            vectorPerCluster,
            random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
            DenseVectorFieldMapper.ElementType.BYTE,
            false,
            null,
            1,
            false,
            DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
            sliceField
        );
        int dimensions = random().nextInt(12, 500);
        int slices = random().nextInt(2, 20);
        int numDocs = random().nextInt(100, 2_000);
        int[] docsPerSlice = new int[slices];
        int[] docSlices = new int[numDocs];
        IndexWriterConfig iwc = newIndexWriterConfig();
        SortField sliceSortField = new SortField(sliceField, SortField.Type.STRING, false, SortField.STRING_LAST);
        iwc.setIndexSort(new Sort(sliceSortField));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(slicedFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(slices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + slice)));
                doc.add(new StoredField(sliceField, new BytesRef("" + slice)));
                byte[] vector = randomByteVector(dimensions);
                doc.add(new KnnByteVectorField("vector", vector, VectorSimilarityFunction.EUCLIDEAN));
                docsPerSlice[slice]++;
                w.addDocument(doc);
                docSlices[i] = slice;
            }
            w.commit();
            if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            byte[] queryVector = randomByteVector(dimensions);
            try (IndexReader reader = DirectoryReader.open(w)) {
                var searcher = new org.apache.lucene.search.IndexSearcher(reader);
                for (int slice = 0; slice < slices; slice++) {
                    int expectedDocs = docsPerSlice[slice];
                    Query query = SortedDocValuesField.newSlowExactQuery(sliceField, new BytesRef("" + slice));
                    var weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                    TopDocs[] topDocsArray = new TopDocs[reader.leaves().size()];
                    for (int i = 0; i < reader.leaves().size(); i++) {
                        LeafReaderContext context = reader.leaves().get(i);
                        LeafReader leafReader = context.reader();
                        int ord = leafReader.getSortedDocValues(sliceField).lookupTerm(new BytesRef("" + slice));
                        if (ord < 0) {
                            topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                            continue;
                        }
                        ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
                        DocIdSetIterator iterator = scorerSupplier.get(DocIdSetIterator.NO_MORE_DOCS).iterator();
                        int minDoc = iterator.nextDoc();
                        if (minDoc == DocIdSetIterator.NO_MORE_DOCS) {
                            topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                            continue;
                        }
                        int maxDoc = minDoc;
                        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            maxDoc = iterator.docID();
                        }
                        ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = new ESAcceptDocs.SliceAcceptDocs(minDoc, maxDoc + 1);
                        ESAcceptDocs acceptDocs = new ESAcceptDocs.ESAcceptDocsAll(ord, () -> sliceAcceptDocs);
                        KnnCollector collector = new TopKnnCollector(2 * Math.max(1, expectedDocs), Integer.MAX_VALUE);
                        leafReader.searchNearestVectors("vector", queryVector, collector, acceptDocs);
                        TopDocs leafTopDocs = collector.topDocs();
                        ScoreDoc[] adjusted = new ScoreDoc[leafTopDocs.scoreDocs.length];
                        for (int docIndex = 0; docIndex < leafTopDocs.scoreDocs.length; docIndex++) {
                            ScoreDoc scoreDoc = leafTopDocs.scoreDocs[docIndex];
                            adjusted[docIndex] = new ScoreDoc(scoreDoc.doc + context.docBase, scoreDoc.score);
                        }
                        topDocsArray[i] = new TopDocs(leafTopDocs.totalHits, adjusted);
                    }
                    TopDocs topDocs = TopDocs.merge(2 * expectedDocs, topDocsArray);
                    Set<Integer> uniqueDocIds = new HashSet<>();
                    for (int j = 0; j < topDocs.scoreDocs.length; j++) {
                        uniqueDocIds.add(topDocs.scoreDocs[j].doc);
                        Document document = reader.storedFields().document(topDocs.scoreDocs[j].doc);
                        assertThat(document.getField(sliceField).binaryValue().utf8ToString(), equalTo("" + slice));
                    }
                    assertThat(uniqueDocIds, hasSize(expectedDocs));
                }
            }
        }
    }
}
