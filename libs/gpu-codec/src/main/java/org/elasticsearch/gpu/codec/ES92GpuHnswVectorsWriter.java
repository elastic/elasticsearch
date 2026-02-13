/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.reflect.VectorsFormatReflectionUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter.mergeAndRecalculateQuantiles;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.LUCENE99_HNSW_META_CODEC_NAME;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.LUCENE99_HNSW_META_EXTENSION;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_EXTENSION;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.LUCENE99_VERSION_CURRENT;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.MIN_NUM_VECTORS_FOR_GPU_BUILD;
import static org.elasticsearch.gpu.codec.MemorySegmentUtils.getContiguousMemorySegment;
import static org.elasticsearch.gpu.codec.MemorySegmentUtils.getContiguousPackedMemorySegment;

/**
 * Writer that builds an Nvidia Carga Graph on GPU and then writes it into the Lucene99 HNSW format,
 * so that it can be searched on CPU with Lucene99HNSWVectorReader.
 */
final class ES92GpuHnswVectorsWriter extends KnnVectorsWriter {
    private static final Logger logger = LogManager.getLogger(ES92GpuHnswVectorsWriter.class);
    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ES92GpuHnswVectorsWriter.class);
    private static final int LUCENE99_HNSW_DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private static final long DIRECT_COPY_THRESHOLD_IN_BYTES = 128 * 1024 * 1024; // 128MB

    // TODO: lower the numVectors threshold when to switch to IVF_PQ based on more benchmarks
    private static final long MAX_NUM_VECTORS_FOR_NN_DESCENT = 5_000_000L;

    private final CuVSResourceManager cuVSResourceManager;
    private final SegmentWriteState segmentWriteState;
    private final IndexOutput meta, vectorIndex;
    private final int M;
    private final int beamWidth;
    private final FlatVectorsWriter flatVectorWriter;

    private final List<FieldWriter> fields = new ArrayList<>();
    private boolean finished;
    private final CuVSMatrix.DataType dataType;

    ES92GpuHnswVectorsWriter(
        CuVSResourceManager cuVSResourceManager,
        SegmentWriteState state,
        int M,
        int beamWidth,
        FlatVectorsWriter flatVectorWriter
    ) throws IOException {
        assert cuVSResourceManager != null : "CuVSResources must not be null";
        this.cuVSResourceManager = cuVSResourceManager;
        this.M = M;
        this.beamWidth = beamWidth;
        this.flatVectorWriter = flatVectorWriter;
        if (flatVectorWriter instanceof ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter) {
            dataType = CuVSMatrix.DataType.BYTE;
        } else {
            assert flatVectorWriter instanceof Lucene99FlatVectorsWriter;
            dataType = CuVSMatrix.DataType.FLOAT;
        }
        this.segmentWriteState = state;
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, LUCENE99_HNSW_META_EXTENSION);
        String indexDataFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            LUCENE99_HNSW_VECTOR_INDEX_EXTENSION
        );
        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            vectorIndex = state.directory.createOutput(indexDataFileName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                LUCENE99_HNSW_META_CODEC_NAME,
                LUCENE99_VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.writeIndexHeader(
                vectorIndex,
                LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME,
                LUCENE99_VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                org.elasticsearch.core.IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) == false) {
            throw new IllegalArgumentException(
                "Field [" + fieldInfo.name + "] must have FLOAT32 encoding, got: " + fieldInfo.getVectorEncoding()
            );
        }
        @SuppressWarnings("unchecked")
        FlatFieldVectorsWriter<float[]> flatFieldWriter = (FlatFieldVectorsWriter<float[]>) flatVectorWriter.addField(fieldInfo);
        FieldWriter newField = new FieldWriter(flatFieldWriter, fieldInfo);
        fields.add(newField);
        return newField;
    }

    /**
     * Flushes vector data and associated data to disk.
     * <p>
     * This method and the private helpers it calls only need to support FLOAT32.
     * For FlatFieldVectorWriter we only need to support float[] during flush: during indexing users provide floats[], and pass floats to
     * FlatFieldVectorWriter, even when we have a BYTE dataType (i.e. an "int8_hnsw" type).
     * During merging, we use quantized data, so we need to support byte[] too (see {@link ES92GpuHnswVectorsWriter#mergeOneField}),
     * but not here.
     * That's how our other current formats work: use floats during indexing, and quantized data to build graph during merging.
     * </p>
     */
    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        var started = System.nanoTime();
        flatVectorWriter.flush(maxDoc, sortMap);
        try {
            flushFieldsWithoutMemoryMappedFile(sortMap);
        } catch (Throwable t) {
            throw new IOException("Failed to flush GPU index: ", t);
        }
        var elapsed = System.nanoTime() - started;
        logger.debug("Flush total time [{}ms]", elapsed / 1_000_000.0);
    }

    private void flushFieldsWithoutMemoryMappedFile(Sorter.DocMap sortMap) throws IOException, InterruptedException {
        // No tmp file written, or the file cannot be mmapped
        for (FieldWriter field : fields) {
            var started = System.nanoTime();
            var fieldInfo = field.fieldInfo;

            var originalVectors = field.flatFieldVectorsWriter.getVectors();
            final List<float[]> vectorsInSortedOrder = sortMap == null
                ? originalVectors
                : getVectorsInSortedOrder(field, sortMap, originalVectors);
            int numVectors = vectorsInSortedOrder.size();
            CagraIndexParams cagraIndexParams = createCagraIndexParams(
                fieldInfo.getVectorSimilarityFunction(),
                numVectors,
                fieldInfo.getVectorDimension()
            );

            if (numVectors < MIN_NUM_VECTORS_FOR_GPU_BUILD) {
                logger.debug("Skip building carga index; vectors length {} < {} (min for GPU)", numVectors, MIN_NUM_VECTORS_FOR_GPU_BUILD);
                // Will not be indexed on the GPU
                generateMockGraphAndWriteMeta(fieldInfo, numVectors);
            } else {
                try (
                    var resourcesHolder = new ResourcesHolder(
                        cuVSResourceManager,
                        cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), CuVSMatrix.DataType.FLOAT, cagraIndexParams)
                    )
                ) {
                    var builder = CuVSMatrix.deviceBuilder(
                        resourcesHolder.resources(),
                        numVectors,
                        fieldInfo.getVectorDimension(),
                        CuVSMatrix.DataType.FLOAT
                    );
                    for (var vector : vectorsInSortedOrder) {
                        builder.addVector(vector);
                    }
                    try (var dataset = builder.build()) {
                        generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
                    }
                }
            }
            var elapsed = System.nanoTime() - started;
            logger.debug("Flushed [{}] vectors in [{}ms]", numVectors, elapsed / 1_000_000.0);
        }
    }

    private List<float[]> getVectorsInSortedOrder(FieldWriter field, Sorter.DocMap sortMap, List<float[]> originalVectors)
        throws IOException {
        DocsWithFieldSet docsWithField = field.getDocsWithFieldSet();
        int[] ordMap = new int[docsWithField.cardinality()];
        DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
        KnnVectorsWriter.mapOldOrdToNewOrd(docsWithField, sortMap, null, ordMap, newDocsWithField);
        List<float[]> vectorsInSortedOrder = new ArrayList<>(ordMap.length);
        for (int oldOrd : ordMap) {
            vectorsInSortedOrder.add(originalVectors.get(oldOrd));
        }
        return vectorsInSortedOrder;
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        flatVectorWriter.finish();

        if (meta != null) {
            // write end of fields marker
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
        }
        if (vectorIndex != null) {
            CodecUtil.writeFooter(vectorIndex);
        }
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (FieldWriter field : fields) {
            // the field tracks the delegate field usage
            total += field.ramBytesUsed();
        }
        return total;
    }

    private void generateGpuGraphAndWriteMeta(
        ResourcesHolder resourcesHolder,
        FieldInfo fieldInfo,
        CuVSMatrix dataset,
        CagraIndexParams cagraIndexParams
    ) throws IOException {
        try {
            assert dataset.size() >= MIN_NUM_VECTORS_FOR_GPU_BUILD;

            long vectorIndexOffset = vectorIndex.getFilePointer();
            int[][] graphLevelNodeOffsets = new int[1][];
            final HnswGraph graph;
            try (var index = buildGPUIndex(resourcesHolder.resources(), cagraIndexParams, dataset, fieldInfo)) {
                assert index != null : "GPU index should be built for field: " + fieldInfo.name;
                var deviceGraph = index.getGraph();
                var graphSize = deviceGraph.size() * deviceGraph.columns() * Integer.BYTES;
                if (graphSize < DIRECT_COPY_THRESHOLD_IN_BYTES) {
                    // If the graph is "small enough", copy it entirely to host memory so we can
                    // release the associated resource early and increase parallelism.
                    try (var hostGraph = deviceGraph.toHost()) {
                        resourcesHolder.close();
                        graph = writeGraph(hostGraph, graphLevelNodeOffsets);
                    }
                } else {
                    graph = writeGraph(deviceGraph, graphLevelNodeOffsets);
                }
            }
            long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
            writeMeta(fieldInfo, vectorIndexOffset, vectorIndexLength, (int) dataset.size(), graph, graphLevelNodeOffsets);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to write GPU index: ", t);
        }
    }

    private void generateMockGraphAndWriteMeta(FieldInfo fieldInfo, int datasetSize) throws IOException {
        try {
            long vectorIndexOffset = vectorIndex.getFilePointer();
            int[][] graphLevelNodeOffsets = new int[1][];
            final HnswGraph graph = writeMockGraph(datasetSize, graphLevelNodeOffsets);
            long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
            writeMeta(fieldInfo, vectorIndexOffset, vectorIndexLength, datasetSize, graph, graphLevelNodeOffsets);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to write GPU index: ", t);
        }
    }

    private CagraIndex buildGPUIndex(
        CuVSResourceManager.ManagedCuVSResources cuVSResources,
        CagraIndexParams cagraIndexParams,
        CuVSMatrix dataset,
        FieldInfo fieldInfo
    ) throws Throwable {
        if (logger.isDebugEnabled()) {
            var algorithm = cagraIndexParams.getCagraGraphBuildAlgo();
            if (algorithm == CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT) {
                logger.debug(
                    "Building CAGRA graph: numVectors=[{}], dims=[{}], algorithm=[{}], similarity=[{}], "
                        + "graphDegree=[{}], intermediateGraphDegree=[{}], nnDescentIterations=[5], dataType=[{}]",
                    dataset.size(),
                    dataset.columns(),
                    algorithm,
                    fieldInfo.getVectorSimilarityFunction(),
                    M,
                    beamWidth,
                    dataType
                );
            } else {
                // IVF_PQ algorithm
                var ivfPqIndexParams = cagraIndexParams.getCuVSIvfPqParams().getIndexParams();
                logger.debug(
                    "Building CAGRA graph: numVectors=[{}], dims=[{}], algorithm=[{}], similarity=[{}], "
                        + "pqDim=[{}], pqBits=[{}], nLists=[{}], dataType=[{}]",
                    dataset.size(),
                    dataset.columns(),
                    algorithm,
                    fieldInfo.getVectorSimilarityFunction(),
                    ivfPqIndexParams.getPqDim(),
                    ivfPqIndexParams.getPqBits(),
                    ivfPqIndexParams.getnLists(),
                    dataType
                );
            }
        }
        long startTime = System.nanoTime();
        var indexBuilder = CagraIndex.newBuilder(cuVSResources).withDataset(dataset).withIndexParams(cagraIndexParams);
        var index = indexBuilder.build();
        GPUSupport.incrementUsageCount();
        cuVSResourceManager.finishedComputation(cuVSResources);
        if (logger.isDebugEnabled()) {
            logger.debug("Carga index created in: {} ms; #num vectors: {}", (System.nanoTime() - startTime) / 1_000_000.0, dataset.size());
        }
        return index;
    }

    private CagraIndexParams createCagraIndexParams(VectorSimilarityFunction similarityFunction, int numVectors, int dims) {
        CagraIndexParams.CuvsDistanceType distanceType = switch (similarityFunction) {
            case COSINE -> CagraIndexParams.CuvsDistanceType.CosineExpanded;
            case EUCLIDEAN -> CagraIndexParams.CuvsDistanceType.L2Expanded;
            case DOT_PRODUCT -> {
                if (dataType == CuVSMatrix.DataType.BYTE) {
                    yield CagraIndexParams.CuvsDistanceType.CosineExpanded;
                }
                yield CagraIndexParams.CuvsDistanceType.InnerProduct;
            }
            case MAXIMUM_INNER_PRODUCT -> {
                assert dataType != CuVSMatrix.DataType.BYTE;
                yield CagraIndexParams.CuvsDistanceType.InnerProduct;
            }
        };

        int numCPUThreads = 1; // TODO: how many CPU threads we can use?
        CagraIndexParams params;

        boolean useIvfPQ = false;
        if (numVectors >= MAX_NUM_VECTORS_FOR_NN_DESCENT) {
            useIvfPQ = true;
        } else {
            // Check if we should use IVF_PQ due to insufficient GPU memory for NN_DESCENT
            long totalDeviceMemory = GPUSupport.getTotalGpuMemory();
            if (totalDeviceMemory > 0) {
                long requiredMemoryForNnDescent = CuVSResourceManager.estimateNNDescentMemory(numVectors, dims, dataType);
                if (requiredMemoryForNnDescent > totalDeviceMemory) {
                    useIvfPQ = true;
                    logger.debug(
                        "Using IVF_PQ algorithm due to insufficient GPU memory for NN_DESCENT; required [{}B] > total [{}B]",
                        requiredMemoryForNnDescent,
                        totalDeviceMemory
                    );
                }
            }
        }

        if (useIvfPQ) {
            var ivfPqParams = CuVSIvfPqParamsFactory.create(numVectors, dims, distanceType, beamWidth);
            params = new CagraIndexParams.Builder().withNumWriterThreads(numCPUThreads)
                .withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.IVF_PQ)
                .withCuVSIvfPqParams(ivfPqParams)
                .withMetric(distanceType)
                .build();
        } else {
            params = new CagraIndexParams.Builder().withNumWriterThreads(numCPUThreads)
                .withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT)
                .withGraphDegree(M)
                .withIntermediateGraphDegree(beamWidth)
                .withNNDescentNumIterations(5)
                .withMetric(distanceType)
                .build();
        }
        return params;
    }

    private HnswGraph writeGraph(CuVSMatrix cagraGraph, int[][] levelNodeOffsets) throws IOException {
        long startTime = System.nanoTime();

        int maxElementCount = (int) cagraGraph.size();
        int maxGraphDegree = (int) cagraGraph.columns();
        int[] neighbors = new int[maxGraphDegree];

        levelNodeOffsets[0] = new int[maxElementCount];
        // write the cagra graph to the Lucene vectorIndex file
        int[] scratch = new int[maxGraphDegree];
        for (int node = 0; node < maxElementCount; node++) {
            cagraGraph.getRow(node).toArray(neighbors);

            // write to the Lucene vectorIndex file
            long offsetStart = vectorIndex.getFilePointer();
            Arrays.sort(neighbors);
            int actualSize = 0;
            if (maxGraphDegree > 0) {
                scratch[0] = neighbors[0];
                actualSize = 1;
            }
            for (int i = 1; i < maxGraphDegree; i++) {
                assert neighbors[i] < maxElementCount : "node too large: " + neighbors[i] + ">=" + maxElementCount;
                if (neighbors[i - 1] == neighbors[i]) {
                    continue;
                }
                scratch[actualSize++] = neighbors[i] - neighbors[i - 1];
            }
            // Write the size after duplicates are removed
            vectorIndex.writeVInt(actualSize);
            vectorIndex.writeGroupVInts(scratch, actualSize);
            levelNodeOffsets[0][node] = Math.toIntExact(vectorIndex.getFilePointer() - offsetStart);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("cagra_hnws index serialized to Lucene HNSW in: {} ms", (System.nanoTime() - startTime) / 1_000_000.0);
        }
        return createMockGraph(maxElementCount, maxGraphDegree);
    }

    // create a mock graph where every node is connected to every other node
    private HnswGraph writeMockGraph(int elementCount, int[][] levelNodeOffsets) throws IOException {
        if (elementCount == 0) {
            return null;
        }
        int nodeDegree = elementCount - 1;
        levelNodeOffsets[0] = new int[elementCount];

        int[] neighbors = new int[nodeDegree];
        int[] scratch = new int[nodeDegree];
        for (int node = 0; node < elementCount; node++) {
            if (nodeDegree > 0) {
                for (int j = 0; j < nodeDegree; j++) {
                    neighbors[j] = j < node ? j : j + 1; // skip self
                }
                scratch[0] = neighbors[0];
                for (int i = 1; i < nodeDegree; i++) {
                    scratch[i] = neighbors[i] - neighbors[i - 1];
                }
            }

            long offsetStart = vectorIndex.getFilePointer();
            vectorIndex.writeVInt(nodeDegree);
            vectorIndex.writeGroupVInts(scratch, nodeDegree);
            levelNodeOffsets[0][node] = Math.toIntExact(vectorIndex.getFilePointer() - offsetStart);
        }
        return createMockGraph(elementCount, nodeDegree);
    }

    private static HnswGraph createMockGraph(int elementCount, int graphDegree) {
        return new HnswGraph() {
            @Override
            public int nextNeighbor() {
                throw new UnsupportedOperationException("Not supported on a mock graph");
            }

            @Override
            public void seek(int level, int target) {
                throw new UnsupportedOperationException("Not supported on a mock graph");
            }

            @Override
            public int size() {
                return elementCount;
            }

            @Override
            public int numLevels() {
                return 1;
            }

            @Override
            public int maxConn() {
                return graphDegree;
            }

            @Override
            public int entryNode() {
                throw new UnsupportedOperationException("Not supported on a mock graph");
            }

            @Override
            public int neighborCount() {
                throw new UnsupportedOperationException("Not supported on a mock graph");
            }

            @Override
            public NodesIterator getNodesOnLevel(int level) {
                return new ArrayNodesIterator(size());
            }
        };
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        // Note: Merged raw vectors are already in sorted order. The flatVectorWriter and MergedVectorValues utilities
        // apply mergeState.docMaps internally, so vectors are returned in the final sorted document order.
        // Unlike flush(), we don't need to explicitly handle sorting here.
        try (var scorerSupplier = flatVectorWriter.mergeOneFieldToIndex(fieldInfo, mergeState)) {
            var started = System.nanoTime();
            int numVectors = scorerSupplier.totalVectorCount();
            if (numVectors < MIN_NUM_VECTORS_FOR_GPU_BUILD) {
                // we don't really need real value for vectors here,
                // we just build a mock graph where every node is connected to every other node
                generateMockGraphAndWriteMeta(fieldInfo, numVectors);
            } else {
                if (dataType == CuVSMatrix.DataType.FLOAT) {
                    var randomScorerSupplier = VectorsFormatReflectionUtils.getFlatRandomVectorScorerInnerSupplier(scorerSupplier);
                    mergeFloatVectorField(fieldInfo, mergeState, randomScorerSupplier, numVectors);
                } else {
                    // During merging, we use quantized data, so we need to support byte[] too.
                    // That's how our current formats work: use floats during indexing, and quantized data to build a graph
                    // during merging.
                    assert dataType == CuVSMatrix.DataType.BYTE;
                    var randomScorerSupplier = VectorsFormatReflectionUtils.getScalarQuantizedRandomVectorScorerInnerSupplier(
                        scorerSupplier
                    );
                    mergeByteVectorField(fieldInfo, mergeState, randomScorerSupplier, numVectors);
                }
            }
            var elapsed = System.nanoTime() - started;
            logger.debug("Merged [{}] vectors in [{}ms]", numVectors, elapsed / 1_000_000.0);
        } catch (Throwable t) {
            throw new IOException("Failed to merge GPU index: ", t);
        }
    }

    private void mergeByteVectorField(
        FieldInfo fieldInfo,
        MergeState mergeState,
        RandomVectorScorerSupplier randomScorerSupplier,
        int numVectors
    ) throws IOException, InterruptedException {
        var vectorValues = randomScorerSupplier == null
            ? null
            : VectorsFormatReflectionUtils.getByteScoringSupplierVectorOrNull(randomScorerSupplier);

        CagraIndexParams cagraIndexParams = createCagraIndexParams(
            fieldInfo.getVectorSimilarityFunction(),
            numVectors,
            fieldInfo.getVectorDimension()
        );

        if (vectorValues != null) {
            IndexInput slice = vectorValues.getSlice();
            var input = FilterIndexInput.unwrapOnlyTest(slice);
            if (input instanceof MemorySegmentAccessInput memorySegmentAccessInput) {
                // Direct access to mmapped file
                // for int8_hnsw, the raw vector data has extra 4-byte at the end of each vector to encode a correction constant
                int sourceRowPitch = fieldInfo.getVectorDimension() + 4;

                // The current (25.10) CuVS implementation of CAGRA index build has problems with strides;
                // the explicit copy removes them.
                // TODO: revert to directly pass data mapped with DatasetUtils.getInstance() to generateGpuGraphAndWriteMeta
                // when cuvs has fixed this problem
                int packedRowSize = fieldInfo.getVectorDimension();
                try (
                    var packedSegmentHolder = getContiguousPackedMemorySegment(
                        memorySegmentAccessInput,
                        mergeState.segmentInfo.dir,
                        mergeState.segmentInfo.name,
                        numVectors,
                        sourceRowPitch,
                        packedRowSize
                    );
                    var dataset = DatasetUtilsImpl.fromMemorySegment(
                        packedSegmentHolder.memorySegment(),
                        numVectors,
                        packedRowSize,
                        dataType
                    );
                    var resourcesHolder = new ResourcesHolder(
                        cuVSResourceManager,
                        cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                    )
                ) {
                    generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
                }
            } else {
                logger.info(
                    () -> "Cannot mmap merged raw vectors temporary file. IndexInput type [" + input.getClass().getSimpleName() + "]"
                );

                // TODO: revert to CuVSMatrix.deviceBuilder when cuvs has fixed the multiple copies problem
                var builder = CuVSMatrix.hostBuilder(numVectors, fieldInfo.getVectorDimension(), dataType);

                try (IndexInput clonedSlice = slice.clone()) {
                    clonedSlice.seek(0);
                    byte[] vector = new byte[fieldInfo.getVectorDimension()];
                    for (int i = 0; i < numVectors; ++i) {
                        clonedSlice.readBytes(vector, 0, fieldInfo.getVectorDimension());
                        builder.addVector(vector);
                    }
                }

                try (
                    var dataset = builder.build();
                    var resourcesHolder = new ResourcesHolder(
                        cuVSResourceManager,
                        cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                    )
                ) {
                    generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
                }
            }
        } else {
            logger.warn("Cannot get merged raw vectors from scorer. Performances will be degraded.");
            var byteVectorValues = getMergedByteVectorValues(fieldInfo, mergeState);

            // TODO: revert to CuVSMatrix.deviceBuilder when cuvs has fixed the multiple copies problem
            final var builder = CuVSMatrix.hostBuilder(numVectors, fieldInfo.getVectorDimension(), dataType);
            final KnnVectorValues.DocIndexIterator iterator = byteVectorValues.iterator();
            for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
                builder.addVector(byteVectorValues.vectorValue(iterator.index()));
            }

            try (
                var dataset = builder.build();
                var resourcesHolder = new ResourcesHolder(
                    cuVSResourceManager,
                    cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                )
            ) {
                generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
            }
        }
    }

    private void mergeFloatVectorField(
        FieldInfo fieldInfo,
        MergeState mergeState,
        RandomVectorScorerSupplier randomScorerSupplier,
        final int numVectors
    ) throws IOException, InterruptedException {
        var vectorValues = randomScorerSupplier == null
            ? null
            : VectorsFormatReflectionUtils.getFloatScoringSupplierVectorOrNull(randomScorerSupplier);
        CagraIndexParams cagraIndexParams = createCagraIndexParams(
            fieldInfo.getVectorSimilarityFunction(),
            numVectors,
            fieldInfo.getVectorDimension()
        );

        if (vectorValues != null) {
            IndexInput slice = vectorValues.getSlice();
            var input = FilterIndexInput.unwrapOnlyTest(slice);
            if (input instanceof MemorySegmentAccessInput memorySegmentAccessInput) {
                // Fast path, possible direct access to mmapped file
                try (
                    var memorySegmentHolder = getContiguousMemorySegment(
                        memorySegmentAccessInput,
                        mergeState.segmentInfo.dir,
                        mergeState.segmentInfo.name
                    );
                    var dataset = DatasetUtils.getInstance()
                        .fromInput(memorySegmentHolder.memorySegment(), numVectors, fieldInfo.getVectorDimension(), dataType);
                    var resourcesHolder = new ResourcesHolder(
                        cuVSResourceManager,
                        cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                    )
                ) {
                    generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
                }
            } else {
                logger.info(
                    () -> "Cannot mmap merged raw vectors temporary file. IndexInput type [" + input.getClass().getSimpleName() + "]"
                );

                // TODO: revert to CuVSMatrix.deviceBuilder when cuvs has fixed the multiple copies problem
                var builder = CuVSMatrix.hostBuilder(numVectors, fieldInfo.getVectorDimension(), dataType);

                try (IndexInput clonedSlice = slice.clone()) {
                    clonedSlice.seek(0);
                    float[] vector = new float[fieldInfo.getVectorDimension()];
                    for (int i = 0; i < numVectors; ++i) {
                        clonedSlice.readFloats(vector, 0, fieldInfo.getVectorDimension());
                        builder.addVector(vector);
                    }
                }

                try (
                    var dataset = builder.build();
                    var resourcesHolder = new ResourcesHolder(
                        cuVSResourceManager,
                        cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                    )
                ) {
                    generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
                }
            }
        } else {
            logger.warn("Cannot get merged raw vectors from scorer.");
            FloatVectorValues floatVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);

            // TODO: revert to CuVSMatrix.deviceBuilder when cuvs has fixed the multiple copies problem
            var builder = CuVSMatrix.hostBuilder(numVectors, fieldInfo.getVectorDimension(), dataType);

            final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
            for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
                float[] vector = floatVectorValues.vectorValue(iterator.index());
                builder.addVector(vector);
            }
            try (
                var dataset = builder.build();
                var resourcesHolder = new ResourcesHolder(
                    cuVSResourceManager,
                    cuVSResourceManager.acquire(numVectors, fieldInfo.getVectorDimension(), dataType, cagraIndexParams)
                )
            ) {
                generateGpuGraphAndWriteMeta(resourcesHolder, fieldInfo, dataset, cagraIndexParams);
            }
        }
    }

    private ByteVectorValues getMergedByteVectorValues(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        // TODO: expose confidence interval from the format
        final byte bits = 7;
        final Float confidenceInterval = null;
        ScalarQuantizer quantizer = mergeAndRecalculateQuantiles(mergeState, fieldInfo, confidenceInterval, bits);
        return MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(fieldInfo, mergeState, quantizer);
    }

    private void writeMeta(
        FieldInfo field,
        long vectorIndexOffset,
        long vectorIndexLength,
        int count,
        HnswGraph graph,
        int[][] graphLevelNodeOffsets
    ) throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        meta.writeVLong(vectorIndexOffset);
        meta.writeVLong(vectorIndexLength);
        meta.writeVInt(field.getVectorDimension());
        meta.writeInt(count);
        // write graph nodes on each level
        if (graph == null) {
            meta.writeVInt(M);
            meta.writeVInt(0);
        } else {
            meta.writeVInt(graph.maxConn());
            meta.writeVInt(graph.numLevels());
            long valueCount = 0;

            for (int level = 0; level < graph.numLevels(); level++) {
                NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
                valueCount += nodesOnLevel.size();
                if (level > 0) {
                    int[] nol = new int[nodesOnLevel.size()];
                    int numberConsumed = nodesOnLevel.consume(nol);
                    Arrays.sort(nol);
                    assert numberConsumed == nodesOnLevel.size();
                    meta.writeVInt(nol.length); // number of nodes on a level
                    for (int i = nodesOnLevel.size() - 1; i > 0; --i) {
                        nol[i] -= nol[i - 1];
                    }
                    for (int n : nol) {
                        assert n >= 0 : "delta encoding for nodes failed; expected nodes to be sorted";
                        meta.writeVInt(n);
                    }
                } else {
                    assert nodesOnLevel.size() == count : "Level 0 expects to have all nodes";
                }
            }
            long start = vectorIndex.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(LUCENE99_HNSW_DIRECT_MONOTONIC_BLOCK_SHIFT);
            final DirectMonotonicWriter memoryOffsetsWriter = DirectMonotonicWriter.getInstance(
                meta,
                vectorIndex,
                valueCount,
                LUCENE99_HNSW_DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long cumulativeOffsetSum = 0;
            for (int[] levelOffsets : graphLevelNodeOffsets) {
                for (int v : levelOffsets) {
                    memoryOffsetsWriter.add(cumulativeOffsetSum);
                    cumulativeOffsetSum += v;
                }
            }
            memoryOffsetsWriter.finish();
            meta.writeLong(vectorIndex.getFilePointer() - start);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, vectorIndex, flatVectorWriter);
    }

    static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    private static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);

        private final FieldInfo fieldInfo;
        private int lastDocID = -1;
        private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;

        FieldWriter(FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter, FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            this.flatFieldVectorsWriter = Objects.requireNonNull(flatFieldVectorsWriter);
        }

        @Override
        public void addValue(int docID, float[] vectorValue) throws IOException {
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                    "VectorValuesField \""
                        + fieldInfo.name
                        + "\" appears more than once in this document (only one value is allowed per field)"
                );
            }
            flatFieldVectorsWriter.addValue(docID, vectorValue);
            lastDocID = docID;
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + flatFieldVectorsWriter.ramBytesUsed();
        }

        public DocsWithFieldSet getDocsWithFieldSet() {
            return flatFieldVectorsWriter.getDocsWithFieldSet();
        }
    }
}
