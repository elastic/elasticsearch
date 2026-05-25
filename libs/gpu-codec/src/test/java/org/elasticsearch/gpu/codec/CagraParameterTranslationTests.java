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

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.test.ESTestCase;

import static java.util.Locale.ROOT;

/**
 * Tests the full translation chain from Elasticsearch HNSW index parameters (m, ef_construction)
 * through to the actual CAGRA GPU build parameters, using the production code path.
 *
 * Step 1: ES92GpuHnswVectorsFormat translates (m, ef_construction) to (graphDegree, intermediateGraphDegree)
 * Step 2: ES92GpuHnswVectorsWriter creates CagraIndexParams from those translated values
 */
public class CagraParameterTranslationTests extends ESTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    private static final int DEFAULT_NUM_VECTORS = 100_000;
    private static final int DEFAULT_DIMS = 1024;

    // Known GPU memory sizes
    private static final long L4_GPU_MEMORY = 24L * 1024 * 1024 * 1024; // 24 GB
    private static final long RTX_PRO_6000_GPU_MEMORY = 96L * 1024 * 1024 * 1024; // 96 GB

    public void testDefaultParameters() {
        var params = translateAndBuild(16, 100);
        assertEquals(12, params.getGraphDegree());
        assertEquals(22, params.getIntermediateGraphDegree());
        assertEquals(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT, params.getCagraGraphBuildAlgo());
        // 5 + ef_construction / 16 = 5 + 100 / 16 = 11
        assertEquals(11, params.getNNDescentNumIterations());
    }

    public void testUserParameters_m24_ef200() {
        var params = translateAndBuild(24, 200);
        assertEquals(18, params.getGraphDegree());
        assertEquals(42, params.getIntermediateGraphDegree());
    }

    public void testHighRecallParameters_m45_ef290() {
        var params = translateAndBuild(45, 290);
        assertEquals(32, params.getGraphDegree());
        assertEquals(95, params.getIntermediateGraphDegree());
    }

    public void testHighRecallParameters_m69_ef532() {
        var params = translateAndBuild(69, 532);
        assertEquals(48, params.getGraphDegree());
        assertEquals(212, params.getIntermediateGraphDegree());
    }

    public void testHighRecallParameters_m93_ef528() {
        var params = translateAndBuild(93, 528);
        assertEquals(64, params.getGraphDegree());
        assertEquals(284, params.getIntermediateGraphDegree());
    }

    public void testHighRecallParameters_m93_ef800() {
        var params = translateAndBuild(93, 800);
        assertEquals(64, params.getGraphDegree());
        assertEquals(383, params.getIntermediateGraphDegree());
    }

    public void testDotProductSimilarity() {
        var params = translateAndBuild(24, 200, VectorSimilarityFunction.DOT_PRODUCT, CuVSMatrix.DataType.FLOAT);
        assertEquals(CagraIndexParams.CuvsDistanceType.InnerProduct, params.getCuvsDistanceType());
    }

    public void testCosineSimilarity() {
        var params = translateAndBuild(24, 200, VectorSimilarityFunction.COSINE, CuVSMatrix.DataType.FLOAT);
        assertEquals(CagraIndexParams.CuvsDistanceType.CosineExpanded, params.getCuvsDistanceType());
    }

    public void testEuclideanSimilarity() {
        var params = translateAndBuild(24, 200, VectorSimilarityFunction.EUCLIDEAN, CuVSMatrix.DataType.FLOAT);
        assertEquals(CagraIndexParams.CuvsDistanceType.L2Expanded, params.getCuvsDistanceType());
    }

    public void testInt8DotProductUsesCosine() {
        var params = translateAndBuild(24, 200, VectorSimilarityFunction.DOT_PRODUCT, CuVSMatrix.DataType.BYTE);
        assertEquals(CagraIndexParams.CuvsDistanceType.CosineExpanded, params.getCuvsDistanceType());
    }

    public void testL4AlgorithmSelection() {
        var params = translateAndBuild(24, 200, 100_000, 1024, L4_GPU_MEMORY);
        assertEquals(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT, params.getCagraGraphBuildAlgo());

        var largeParams = translateAndBuild(24, 200, 4_000_000, 1024, L4_GPU_MEMORY);
        assertEquals(CagraIndexParams.CagraGraphBuildAlgo.IVF_PQ, largeParams.getCagraGraphBuildAlgo());
    }

    public void testIvfPqGraphDegreeIsSet() {
        var params = translateAndBuild(24, 200, 4_000_000, 1024, L4_GPU_MEMORY);
        assertEquals(CagraIndexParams.CagraGraphBuildAlgo.IVF_PQ, params.getCagraGraphBuildAlgo());
        assertEquals(18, params.getGraphDegree());
        assertEquals(42, params.getIntermediateGraphDegree());
    }

    public void testRtxPro6000AlgorithmSelection() {
        var params = translateAndBuild(24, 200, 4_000_000, 1024, RTX_PRO_6000_GPU_MEMORY);
        assertEquals(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT, params.getCagraGraphBuildAlgo());
    }

    public void testParameterTable() {
        int[][] hnswParams = {
            { 16, 100 },
            { 24, 200 },
            { 32, 200 },
            { 45, 290 },
            { 48, 400 },
            { 64, 400 },
            { 69, 532 },
            { 93, 528 },
            { 93, 800 },
            { 128, 800 }, };

        StringBuilder table = new StringBuilder();
        String header = String.format(ROOT, "%5s | %16s | %13s | %25s%n", "m", "ef_construction", "graphDegree", "intermediateGraphDegree");
        table.append(header);
        table.append("-".repeat(header.length())).append("\n");
        for (int[] c : hnswParams) {
            int m = c[0];
            int ef = c[1];
            var params = translateAndBuild(m, ef);
            table.append(
                String.format(ROOT, "%5d | %16d | %13d | %25d%n", m, ef, params.getGraphDegree(), params.getIntermediateGraphDegree())
            );
        }
        logger.info("HNSW -> CAGRA parameter translation:\n{}", table);
    }

    public void testAlgorithmSelectionTable() {
        int[][] hnswParams = { { 16, 100 }, { 24, 200 }, { 48, 400 }, { 93, 800 }, };
        int[] vectorCounts = { 100_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 10_000_000 };
        int[] dimensions = { 768, 1024 };

        record GpuSpec(String name, long memory) {}
        GpuSpec[] gpus = { new GpuSpec("L4", L4_GPU_MEMORY), new GpuSpec("RTX PRO 6000", RTX_PRO_6000_GPU_MEMORY) };

        int colWidth = 15;
        String colFmt = " %-" + colWidth + "s |";

        for (GpuSpec gpu : gpus) {
            for (int dims : dimensions) {
                StringBuilder table = new StringBuilder();
                table.append(String.format(ROOT, "%12s |", "numVectors"));
                for (int[] hp : hnswParams) {
                    table.append(String.format(ROOT, colFmt, "m=" + hp[0] + " ef=" + hp[1]));
                }
                table.append("\n");
                int totalWidth = 13 + (colWidth + 3) * hnswParams.length;
                table.append("-".repeat(totalWidth)).append("\n");

                for (int numVectors : vectorCounts) {
                    table.append(String.format(ROOT, "%,12d |", numVectors));
                    for (int[] hp : hnswParams) {
                        var params = translateAndBuild(hp[0], hp[1], numVectors, dims, gpu.memory);
                        String algo = params.getCagraGraphBuildAlgo() == CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT
                            ? "NN_DESC"
                            : "IVF_PQ";
                        table.append(String.format(ROOT, colFmt, algo));
                    }
                    table.append("\n");
                }
                logger.info("Algorithm selection -- {} GPU, {} dims, FLOAT:\n{}", gpu.name, dims, table);
            }
        }
    }

    private static CagraIndexParams translateAndBuild(int m, int efConstruction) {
        return translateAndBuild(m, efConstruction, DEFAULT_NUM_VECTORS, DEFAULT_DIMS, L4_GPU_MEMORY);
    }

    private static CagraIndexParams translateAndBuild(
        int m,
        int efConstruction,
        VectorSimilarityFunction similarity,
        CuVSMatrix.DataType dataType
    ) {
        return translateAndBuild(m, efConstruction, similarity, dataType, DEFAULT_NUM_VECTORS, DEFAULT_DIMS, L4_GPU_MEMORY);
    }

    private static CagraIndexParams translateAndBuild(int m, int efConstruction, int numVectors, int dims, long gpuMemory) {
        return translateAndBuild(
            m,
            efConstruction,
            VectorSimilarityFunction.DOT_PRODUCT,
            CuVSMatrix.DataType.FLOAT,
            numVectors,
            dims,
            gpuMemory
        );
    }

    private static CagraIndexParams translateAndBuild(
        int m,
        int efConstruction,
        VectorSimilarityFunction similarity,
        CuVSMatrix.DataType dataType,
        int numVectors,
        int dims,
        long gpuMemory
    ) {
        int graphDegree = ES92GpuHnswVectorsFormat.cagraGraphDegree(m);
        int intermediateGraphDegree = ES92GpuHnswVectorsFormat.cagraIntermediateGraphDegree(m, efConstruction);
        int nnDescentNumIterations = ES92GpuHnswVectorsFormat.cagraNNDescentNumIterations(efConstruction);
        return ES92GpuHnswVectorsWriter.createCagraIndexParams(
            similarity,
            numVectors,
            dims,
            graphDegree,
            intermediateGraphDegree,
            nnDescentNumIterations,
            dataType,
            gpuMemory
        );
    }
}
