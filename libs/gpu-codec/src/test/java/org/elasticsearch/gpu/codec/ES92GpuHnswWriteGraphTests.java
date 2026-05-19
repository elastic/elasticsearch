/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase.randomVector;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;

/**
 * Validates that the CPU fallback path in {@link ES92GpuHnswVectorsWriter} produces graph
 * data identical to Lucene's {@link Lucene99HnswVectorsFormat}. No GPU required.
 */
public class ES92GpuHnswWriteGraphTests extends ESTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    private static final String FIELD = "vec";
    private int numVectors;
    private int dims;
    private VectorSimilarityFunction similarity;

    @Before
    public void setup() {
        numVectors = randomIntBetween(100, 500);
        dims = randomIntBetween(128, 1024);
        similarity = randomSimilarity();
    }

    public void testCpuFallbackGraphMatchesLucene() throws Exception {
        float[][] vectors = generateVectors(numVectors, dims);
        var gpuFormat = new ES92GpuHnswVectorsFormat(() -> new NullResourceManager(), 0L, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
        var luceneFormat = new Lucene99HnswVectorsFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, 0);

        try (Directory gpuDir = newDirectory(); Directory luceneDir = newDirectory()) {
            indexVectors(gpuDir, gpuFormat, vectors, similarity);
            indexVectors(luceneDir, luceneFormat, vectors, similarity);
            try (DirectoryReader gpuReader = DirectoryReader.open(gpuDir); DirectoryReader luceneReader = DirectoryReader.open(luceneDir)) {
                HnswGraph gpuGraph = getGraph(gpuReader);
                HnswGraph luceneGraph = getGraph(luceneReader);
                assertGraphsEqual(gpuGraph, luceneGraph);
                assertVexBytesEqual(gpuDir, luceneDir);
            }
        }
    }

    public void testCpuFallbackSQGraphMatchesLucene() throws Exception {
        var sqSimilarity = randomValueOtherThan(
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
            ES92GpuHnswWriteGraphTests::randomSimilarity
        );
        float[][] vectors = generateVectors(numVectors, dims);
        var gpuFormat = new ES92GpuHnswSQVectorsFormat(
            () -> new NullResourceManager(),
            0L,
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            null,
            7,
            false
        );
        var luceneFormat = new ES814HnswScalarQuantizedRWVectorsFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);

        try (Directory gpuDir = newDirectory(); Directory luceneDir = newDirectory()) {
            indexVectors(gpuDir, gpuFormat, vectors, sqSimilarity);
            indexVectors(luceneDir, luceneFormat, vectors, sqSimilarity);
            try (DirectoryReader gpuReader = DirectoryReader.open(gpuDir); DirectoryReader luceneReader = DirectoryReader.open(luceneDir)) {
                HnswGraph gpuGraph = getGraph(gpuReader);
                HnswGraph luceneGraph = getGraph(luceneReader);
                assertGraphsEqual(gpuGraph, luceneGraph);
                assertVexBytesEqual(gpuDir, luceneDir);
            }
        }
    }

    private void indexVectors(Directory dir, KnnVectorsFormat format, float[][] vectors, VectorSimilarityFunction similarity)
        throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        iwc.setUseCompoundFile(false);
        try (var writer = new IndexWriter(dir, iwc)) {
            for (float[] vec : vectors) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(FIELD, vec, similarity));
                writer.addDocument(doc);
            }
        }
    }

    private static HnswGraph getGraph(DirectoryReader reader) throws IOException {
        CodecReader codecReader = (CodecReader) getOnlyLeafReader(reader);
        KnnVectorsReader knnReader = codecReader.getVectorReader();
        while (knnReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader) {
            knnReader = perFieldReader.getFieldReader(FIELD);
        }
        return ((HnswGraphProvider) knnReader).getGraph(FIELD);
    }

    private void assertGraphsEqual(HnswGraph gpuGraph, HnswGraph luceneGraph) throws IOException {
        assertEquals("numLevels", luceneGraph.numLevels(), gpuGraph.numLevels());
        assertEquals("size", luceneGraph.size(), gpuGraph.size());
        assertEquals("maxConn", luceneGraph.maxConn(), gpuGraph.maxConn());

        for (int level = 0; level < luceneGraph.numLevels(); level++) {
            var luceneNodes = luceneGraph.getNodesOnLevel(level);
            var gpuNodes = gpuGraph.getNodesOnLevel(level);
            assertEquals("nodes on level " + level, luceneNodes.size(), gpuNodes.size());

            while (luceneNodes.hasNext()) {
                assertTrue("gpu should have more nodes on level " + level, gpuNodes.hasNext());
                int luceneNode = luceneNodes.nextInt();
                int gpuNode = gpuNodes.nextInt();
                assertEquals("node id on level " + level, luceneNode, gpuNode);

                int[] luceneNeighbors = getSortedNeighbors(luceneGraph, level, luceneNode);
                int[] gpuNeighbors = getSortedNeighbors(gpuGraph, level, gpuNode);
                assertArrayEquals("neighbors of node " + luceneNode + " on level " + level, luceneNeighbors, gpuNeighbors);
            }
        }
    }

    private static int[] getSortedNeighbors(HnswGraph graph, int level, int node) throws IOException {
        graph.seek(level, node);
        int count = graph.neighborCount();
        int[] neighbors = new int[count];
        for (int i = 0; i < count; i++) {
            neighbors[i] = graph.nextNeighbor();
        }
        Arrays.sort(neighbors);
        return neighbors;
    }

    private void assertVexBytesEqual(Directory gpuDir, Directory luceneDir) throws IOException {
        String gpuVex = findFileByExtension(gpuDir, "vex");
        String luceneVex = findFileByExtension(luceneDir, "vex");
        byte[] gpuBytes = readVexPayload(gpuDir, gpuVex);
        byte[] luceneBytes = readVexPayload(luceneDir, luceneVex);
        assertArrayEquals(".vex payload (graph + offsets) must be identical between GPU CPU-fallback and Lucene", luceneBytes, gpuBytes);
    }

    private static byte[] readVexPayload(Directory dir, String fileName) throws IOException {
        try (ChecksumIndexInput input = dir.openChecksumInput(fileName)) {
            CodecUtil.checkHeader(
                input,
                "Lucene99HnswVectorsFormatIndex",
                Lucene99HnswVectorsFormat.VERSION_START,
                Lucene99HnswVectorsFormat.VERSION_CURRENT
            );
            input.skipBytes(16);
            int suffixLen = input.readByte() & 0xFF;
            input.skipBytes(suffixLen);
            long headerEnd = input.getFilePointer();
            long payloadLen = input.length() - headerEnd - CodecUtil.footerLength();
            assertTrue("vex file too small to contain header+footer", payloadLen >= 0);
            byte[] bytes = new byte[(int) payloadLen];
            input.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    private static String findFileByExtension(Directory dir, String ext) throws IOException {
        var l = Arrays.stream(dir.listAll()).filter(name -> name.endsWith(ext)).toList();
        assert l.size() == 1 : "expected a list with a single element, got:" + l;
        return l.getFirst();
    }

    private float[][] generateVectors(int count, int dims) {
        float[][] vectors = new float[count][dims];
        for (int i = 0; i < count; i++) {
            vectors[i] = randomVector(dims);
        }
        return vectors;
    }

    private static VectorSimilarityFunction randomSimilarity() {
        return VectorSimilarityFunction.values()[random().nextInt(VectorSimilarityFunction.values().length)];
    }

    /** A {@link CuVSResourceManager} whose {@code tryAcquire} always returns null, forcing the CPU fallback path. */
    static class NullResourceManager implements CuVSResourceManager {
        @Override
        public ManagedCuVSResources acquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) {
            throw new UnsupportedOperationException("NullResourceManager: acquire should not be called");
        }

        @Override
        public ManagedCuVSResources tryAcquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) {
            return null;
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            throw new UnsupportedOperationException("NullResourceManager: finishedComputation should not be called");
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            throw new UnsupportedOperationException("NullResourceManager: release should not be called");
        }

        @Override
        public void shutdown() {}
    }

    static class ES814HnswScalarQuantizedRWVectorsFormat extends ES814HnswScalarQuantizedVectorsFormat {
        ES814HnswScalarQuantizedRWVectorsFormat(int maxConn, int beamWidth) {
            super(maxConn, beamWidth, null, 7, false);
        }

        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat().fieldsWriter(state), 0, null, 0);
        }
    }
}
