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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase.randomVector;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;

/**
 * Validates that the CPU fallback path in {@link ES92GpuHnswVectorsWriter} produces
 * equivalent results to Lucene's {@link Lucene99HnswVectorsFormat}. No GPU required.
 *
 * <p>For float vectors, graph topology is compared structurally (same levels, same nodes
 * on each level) and kNN search results are compared to verify functional equivalence.
 * Neighbor lists are not compared exactly because 1-ULP floating-point differences in
 * vector scoring (due to JIT compilation state) can cause harmless tie-breaking divergence.
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
                assertGraphStructureEqual(getGraph(gpuReader), getGraph(luceneReader));
                assertSearchResultsEqual(gpuReader, luceneReader, vectors);
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
                assertGraphStructureEqual(getGraph(gpuReader), getGraph(luceneReader));
                assertEqualRecallAgainstBruteForce(gpuReader, luceneReader, vectors, sqSimilarity);
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

    /**
     * Asserts that both graphs have the same structure: same number of levels,
     * same nodes on each level. Does not compare neighbor lists, since 1-ULP
     * floating-point differences can cause harmless tie-breaking divergence.
     */
    private void assertGraphStructureEqual(HnswGraph gpuGraph, HnswGraph luceneGraph) throws IOException {
        assertEquals("numLevels", luceneGraph.numLevels(), gpuGraph.numLevels());
        assertEquals("size", luceneGraph.size(), gpuGraph.size());
        assertEquals("maxConn", luceneGraph.maxConn(), gpuGraph.maxConn());

        for (int level = 0; level < luceneGraph.numLevels(); level++) {
            var luceneNodes = luceneGraph.getNodesOnLevel(level);
            var gpuNodes = gpuGraph.getNodesOnLevel(level);
            assertEquals("nodes on level " + level, luceneNodes.size(), gpuNodes.size());
        }
    }

    /**
     * Runs kNN queries against both indexes and asserts that the result sets are identical,
     * verifying that the graphs are functionally equivalent even if neighbor lists differ
     * slightly due to floating-point tie-breaking.
     */
    private void assertSearchResultsEqual(DirectoryReader gpuReader, DirectoryReader luceneReader, float[][] vectors) throws IOException {
        int k = Math.min(10, numVectors);
        int numQueries = Math.min(50, numVectors);
        var gpuSearcher = new IndexSearcher(gpuReader);
        var luceneSearcher = new IndexSearcher(luceneReader);

        for (int q = 0; q < numQueries; q++) {
            float[] queryVec = vectors[q];
            var query = new KnnFloatVectorQuery(FIELD, queryVec, k);
            TopDocs gpuDocs = gpuSearcher.search(query, k);
            TopDocs luceneDocs = luceneSearcher.search(query, k);

            Set<Integer> gpuIds = Arrays.stream(gpuDocs.scoreDocs).map(d -> d.doc).collect(Collectors.toSet());
            Set<Integer> luceneIds = Arrays.stream(luceneDocs.scoreDocs).map(d -> d.doc).collect(Collectors.toSet());
            assertEquals("kNN results for query " + q + " should match", luceneIds, gpuIds);
        }
    }

    /**
     * Computes brute-force exact top-k for each query, then asserts that the GPU index's
     * aggregate recall is within a small tolerance of Lucene's. Quantization means
     * individual queries can legitimately differ, but overall quality should be comparable.
     */
    private void assertEqualRecallAgainstBruteForce(
        DirectoryReader gpuReader,
        DirectoryReader luceneReader,
        float[][] vectors,
        VectorSimilarityFunction sim
    ) throws IOException {
        int k = Math.min(10, numVectors);
        int numQueries = Math.min(50, numVectors);
        var gpuSearcher = new IndexSearcher(gpuReader);
        var luceneSearcher = new IndexSearcher(luceneReader);

        long gpuTotal = 0;
        long luceneTotal = 0;
        for (int q = 0; q < numQueries; q++) {
            float[] queryVec = vectors[q];
            Set<Integer> groundTruth = bruteForceTopK(vectors, queryVec, k, sim);

            var query = new KnnFloatVectorQuery(FIELD, queryVec, k);
            Set<Integer> gpuIds = Arrays.stream(gpuSearcher.search(query, k).scoreDocs).map(d -> d.doc).collect(Collectors.toSet());
            Set<Integer> luceneIds = Arrays.stream(luceneSearcher.search(query, k).scoreDocs).map(d -> d.doc).collect(Collectors.toSet());

            gpuTotal += gpuIds.stream().filter(groundTruth::contains).count();
            luceneTotal += luceneIds.stream().filter(groundTruth::contains).count();
        }
        int totalPossible = numQueries * k;
        double gpuRecall = (double) gpuTotal / totalPossible;
        double luceneRecall = (double) luceneTotal / totalPossible;
        assertTrue(
            "GPU recall (" + gpuRecall + ") should be within 5% of Lucene recall (" + luceneRecall + ")",
            gpuRecall >= luceneRecall - 0.05
        );
    }

    private static Set<Integer> bruteForceTopK(float[][] vectors, float[] query, int k, VectorSimilarityFunction sim) {
        record ScoredDoc(int doc, float score) {}
        ScoredDoc[] scored = new ScoredDoc[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
            scored[i] = new ScoredDoc(i, sim.compare(query, vectors[i]));
        }
        Arrays.sort(scored, (a, b) -> Float.compare(b.score, a.score));
        return Arrays.stream(scored, 0, k).map(ScoredDoc::doc).collect(Collectors.toSet());
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
