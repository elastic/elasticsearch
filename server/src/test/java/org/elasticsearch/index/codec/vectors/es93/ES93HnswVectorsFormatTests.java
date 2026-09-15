/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.simdvec.ESVectorizationProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
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
            random().nextInt(1, 20)
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
            random().nextInt(1, 20)
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES93HnswVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            numMergeWorkers,
            service,
            random().nextInt(1, 20)
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
            hnswGraphThreshold
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
            "ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format=%s)"
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
        var format = new ES93HnswVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 1, null, 0);
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

    @SuppressWarnings("unchecked")
    public void testFloat32GraphBuildUsesNativeScorer() throws IOException {
        int dims = random().nextInt(2, 64);
        float[][] vectors = { randomVector(dims), randomVector(dims) };
        try (Directory dir = newDirectory()) {
            var format = new ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT).flatVectorsFormat();
            try (var writer = format.fieldsWriter(segmentWriteState(dir))) {
                var fieldWriter = (FlatFieldVectorsWriter<float[]>) writer.addField(fieldInfo(dims, VectorEncoding.FLOAT32));
                for (int i = 0; i < vectors.length; i++) {
                    fieldWriter.addValue(i, vectors[i]);
                }
                assertNativeSupplierSelected(
                    fieldWriter.asKnnVectorValues(VectorEncoding.FLOAT32, dims),
                    FloatVectorValues.fromFloats(List.of(vectors), dims)
                );
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testByteGraphBuildUsesNativeScorer() throws IOException {
        int dims = random().nextInt(2, 64);
        byte[][] vectors = { randomVector8(dims), randomVector8(dims) };
        try (Directory dir = newDirectory()) {
            var format = new ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType.BYTE).flatVectorsFormat();
            try (var writer = format.fieldsWriter(segmentWriteState(dir))) {
                var fieldWriter = (FlatFieldVectorsWriter<byte[]>) writer.addField(fieldInfo(dims, VectorEncoding.BYTE));
                for (int i = 0; i < vectors.length; i++) {
                    fieldWriter.addValue(i, vectors[i]);
                }
                assertNativeSupplierSelected(
                    fieldWriter.asKnnVectorValues(VectorEncoding.BYTE, dims),
                    ByteVectorValues.fromBytes(List.of(vectors), dims)
                );
            }
        }
    }

    /**
     * A native scorer that fails to resolve degrades to a Java one with nothing but a log line, so assert the
     * selection directly: the values the graph builder gets must expose a slice, and that must yield a
     * different supplier than equivalent on-heap values, which cannot.
     */
    private static void assertNativeSupplierSelected(KnnVectorValues offHeap, KnnVectorValues onHeap) throws IOException {
        assertThat(offHeap, instanceOf(HasIndexSlice.class));
        var scorer = ES93GenericFlatVectorScorer.INSTANCE;
        var offHeapSupplier = scorer.getRandomVectorScorerSupplier(VectorSimilarityFunction.DOT_PRODUCT, offHeap);
        var onHeapSupplier = scorer.getRandomVectorScorerSupplier(VectorSimilarityFunction.DOT_PRODUCT, onHeap);
        if (ESVectorizationProvider.getInstance().getVectorScorerFactory().usesNative()) {
            assertNotEquals("expected a native supplier for off-heap values", onHeapSupplier.getClass(), offHeapSupplier.getClass());
        }
        var expected = onHeapSupplier.scorer();
        var actual = offHeapSupplier.scorer();
        expected.setScoringOrdinal(0);
        actual.setScoringOrdinal(0);
        assertEquals(expected.score(1), actual.score(1), 1e-5f);
    }

    private static SegmentWriteState segmentWriteState(Directory dir) {
        var segmentInfo = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10_000,
            false,
            false,
            Codec.getDefault(),
            Map.of(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
        return new SegmentWriteState(
            InfoStream.getDefault(),
            dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[0]),
            null,
            newIOContext(random())
        );
    }

    private static FieldInfo fieldInfo(int dims, VectorEncoding encoding) {
        return new FieldInfo(
            "field",
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
            dims,
            encoding,
            VectorSimilarityFunction.DOT_PRODUCT,
            false,
            false
        );
    }
}
