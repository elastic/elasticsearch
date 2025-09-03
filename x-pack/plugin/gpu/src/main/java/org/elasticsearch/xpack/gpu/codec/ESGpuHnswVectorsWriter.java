/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter.mergeAndRecalculateQuantiles;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.LUCENE99_HNSW_META_CODEC_NAME;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.LUCENE99_HNSW_META_EXTENSION;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_EXTENSION;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.LUCENE99_VERSION_CURRENT;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.MIN_NUM_VECTORS_FOR_GPU_BUILD;

/**
 * Writer that builds a Nvidia Carga Graph on GPU and than writes it into the Lucene99 HNSW format,
 * so that it can be searched on CPU with Lucene99HNSWVectorReader.
 */
final class ESGpuHnswVectorsWriter extends KnnVectorsWriter {
    private static final Logger logger = LogManager.getLogger(ESGpuHnswVectorsWriter.class);
    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ESGpuHnswVectorsWriter.class);
    private static final int LUCENE99_HNSW_DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final CuVSResourceManager cuVSResourceManager;
    private final SegmentWriteState segmentWriteState;
    private final IndexOutput meta, vectorIndex;
    private final int M;
    private final int beamWidth;
    private final FlatVectorsWriter flatVectorWriter;

    private final List<FieldWriter> fields = new ArrayList<>();
    private boolean finished;
    private final CuVSMatrix.DataType dataType;

    ESGpuHnswVectorsWriter(
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

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        flatVectorWriter.flush(maxDoc, sortMap);
        for (FieldWriter field : fields) {
            if (sortMap == null) {
                writeField(field);
            } else {
                writeSortingField(field, sortMap);
            }
        }
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

    private static final class DatasetOrVectors {
        private final CuVSMatrix dataset;
        private final float[][] vectors;

        static DatasetOrVectors fromArray(float[][] vectors) {
            return new DatasetOrVectors(
                vectors.length < MIN_NUM_VECTORS_FOR_GPU_BUILD ? null : CuVSMatrix.ofArray(vectors),
                vectors.length < MIN_NUM_VECTORS_FOR_GPU_BUILD ? vectors : null
            );
        }

        static DatasetOrVectors fromDataset(CuVSMatrix dataset) {
            return new DatasetOrVectors(dataset, null);
        }

        private DatasetOrVectors(CuVSMatrix dataset, float[][] vectors) {
            this.dataset = dataset;
            this.vectors = vectors;
            validateState();
        }

        private void validateState() {
            if ((dataset == null && vectors == null) || (dataset != null && vectors != null)) {
                throw new IllegalStateException("Exactly one of dataset or vectors must be non-null");
            }
        }

        int size() {
            return dataset != null ? (int) dataset.size() : vectors.length;
        }

        CuVSMatrix getDataset() {
            return dataset;
        }

        float[][] getVectors() {
            return vectors;
        }
    }

    private void writeField(FieldWriter fieldWriter) throws IOException {
        float[][] vectors = fieldWriter.flatFieldVectorsWriter.getVectors().toArray(float[][]::new);
        writeFieldInternal(fieldWriter.fieldInfo, DatasetOrVectors.fromArray(vectors));
    }

    private void writeSortingField(FieldWriter fieldData, Sorter.DocMap sortMap) throws IOException {
        // The flatFieldVectorsWriter's flush method, called before this, has already sorted the vectors according to the sortMap.
        // We can now treat them as a simple, sorted list of vectors.
        float[][] vectors = fieldData.flatFieldVectorsWriter.getVectors().toArray(float[][]::new);
        writeFieldInternal(fieldData.fieldInfo, DatasetOrVectors.fromArray(vectors));
    }

    private void writeFieldInternal(FieldInfo fieldInfo, DatasetOrVectors datasetOrVectors) throws IOException {
        try {
            long vectorIndexOffset = vectorIndex.getFilePointer();
            int[][] graphLevelNodeOffsets = new int[1][];
            HnswGraph mockGraph;
            if (datasetOrVectors.getVectors() != null) {
                int size = datasetOrVectors.size();
                if (logger.isDebugEnabled()) {
                    logger.debug("Skip building carga index; vectors length {} < {} (min for GPU)", size, MIN_NUM_VECTORS_FOR_GPU_BUILD);
                }
                mockGraph = writeGraph(size, graphLevelNodeOffsets);
            } else {
                var dataset = datasetOrVectors.getDataset();
                var cuVSResources = cuVSResourceManager.acquire((int) dataset.size(), (int) dataset.columns(), dataset.dataType());
                try {
                    try (var index = buildGPUIndex(cuVSResources, fieldInfo.getVectorSimilarityFunction(), dataset)) {
                        assert index != null : "GPU index should be built for field: " + fieldInfo.name;
                        mockGraph = writeGraph(index.getGraph(), graphLevelNodeOffsets);
                    }
                } finally {
                    cuVSResourceManager.release(cuVSResources);
                }
            }
            long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
            writeMeta(fieldInfo, vectorIndexOffset, vectorIndexLength, datasetOrVectors.size(), mockGraph, graphLevelNodeOffsets);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to write GPU index: ", t);
        }
    }

    private CagraIndex buildGPUIndex(
        CuVSResourceManager.ManagedCuVSResources cuVSResources,
        VectorSimilarityFunction similarityFunction,
        CuVSMatrix dataset
    ) throws Throwable {
        CagraIndexParams.CuvsDistanceType distanceType = switch (similarityFunction) {
            case EUCLIDEAN -> CagraIndexParams.CuvsDistanceType.L2Expanded;
            case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> CagraIndexParams.CuvsDistanceType.InnerProduct;
            case COSINE -> CagraIndexParams.CuvsDistanceType.CosineExpanded;
        };

        // TODO: expose cagra index params for algorithm, NNDescentNumIterations
        CagraIndexParams params = new CagraIndexParams.Builder().withNumWriterThreads(1) // TODO: how many CPU threads we can use?
            .withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT)
            .withGraphDegree(M)
            .withIntermediateGraphDegree(beamWidth)
            .withMetric(distanceType)
            .build();

        long startTime = System.nanoTime();
        var indexBuilder = CagraIndex.newBuilder(cuVSResources).withDataset(dataset).withIndexParams(params);
        var index = indexBuilder.build();
        cuVSResourceManager.finishedComputation(cuVSResources);
        if (logger.isDebugEnabled()) {
            logger.debug("Carga index created in: {} ms; #num vectors: {}", (System.nanoTime() - startTime) / 1_000_000.0, dataset.size());
        }
        return index;
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
            scratch[actualSize++] = neighbors[0];
            for (int i = 1; i < maxGraphDegree; i++) {
                assert neighbors[i] < maxElementCount : "node too large: " + neighbors[i] + ">=" + maxElementCount;
                if (neighbors[i - 1] == neighbors[i]) {
                    continue;
                }
                scratch[actualSize++] = neighbors[i] - neighbors[i - 1];
            }
            // Write the size after duplicates are removed
            vectorIndex.writeVInt(actualSize);
            for (int i = 0; i < actualSize; i++) {
                vectorIndex.writeVInt(scratch[i]);
            }
            levelNodeOffsets[0][node] = Math.toIntExact(vectorIndex.getFilePointer() - offsetStart);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("cagra_hnws index serialized to Lucene HNSW in: {} ms", (System.nanoTime() - startTime) / 1_000_000.0);
        }
        return createMockGraph(maxElementCount, maxGraphDegree);
    }

    // create a mock graph where every node is connected to every other node
    private HnswGraph writeGraph(int elementCount, int[][] levelNodeOffsets) throws IOException {
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
            for (int i = 0; i < nodeDegree; i++) {
                vectorIndex.writeVInt(scratch[i]);
            }
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

    // TODO check with deleted documents
    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        flatVectorWriter.mergeOneField(fieldInfo, mergeState);
        final int numVectors;
        String tempRawVectorsFileName = null;
        boolean success = false;
        // save merged vector values to a temp file
        try (IndexOutput out = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "vec_", IOContext.DEFAULT)) {
            tempRawVectorsFileName = out.getName();
            if (dataType == CuVSMatrix.DataType.BYTE) {
                numVectors = writeByteVectorValues(out, getMergedByteVectorValues(fieldInfo, mergeState));
            } else {
                numVectors = writeFloatVectorValues(fieldInfo, out, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
            }
            CodecUtil.writeFooter(out);
            success = true;
        } finally {
            if (success == false && tempRawVectorsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
            }
        }
        try (IndexInput in = mergeState.segmentInfo.dir.openInput(tempRawVectorsFileName, IOContext.DEFAULT)) {
            DatasetOrVectors datasetOrVectors;
            var input = FilterIndexInput.unwrapOnlyTest(in);
            if (input instanceof MemorySegmentAccessInput memorySegmentAccessInput && numVectors >= MIN_NUM_VECTORS_FOR_GPU_BUILD) {
                var ds = DatasetUtils.getInstance()
                    .fromInput(memorySegmentAccessInput, numVectors, fieldInfo.getVectorDimension(), dataType);
                datasetOrVectors = DatasetOrVectors.fromDataset(ds);
            } else {
                assert numVectors < MIN_NUM_VECTORS_FOR_GPU_BUILD : "numVectors: " + numVectors;
                // we don't really need real value for vectors here,
                // we just build a mock graph where every node is connected to every other node
                float[][] vectors = new float[numVectors][fieldInfo.getVectorDimension()];
                datasetOrVectors = DatasetOrVectors.fromArray(vectors);
            }
            writeFieldInternal(fieldInfo, datasetOrVectors);
        } finally {
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
        }
    }

    private ByteVectorValues getMergedByteVectorValues(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        // TODO: expose confidence interval from the format
        final byte bits = 7;
        final Float confidenceInterval = null;
        ScalarQuantizer quantizer = mergeAndRecalculateQuantiles(mergeState, fieldInfo, confidenceInterval, bits);
        MergedQuantizedVectorValues byteVectorValues = MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
            fieldInfo,
            mergeState,
            quantizer
        );
        return byteVectorValues;
    }

    private static int writeByteVectorValues(IndexOutput out, ByteVectorValues vectorValues) throws IOException {
        int numVectors = 0;
        byte[] vector;
        final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            vector = vectorValues.vectorValue(iterator.index());
            out.writeBytes(vector, vector.length);
        }
        return numVectors;
    }

    private static int writeFloatVectorValues(FieldInfo fieldInfo, IndexOutput out, FloatVectorValues floatVectorValues)
        throws IOException {
        int numVectors = 0;
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            float[] vector = floatVectorValues.vectorValue(iterator.index());
            buffer.asFloatBuffer().put(vector);
            out.writeBytes(buffer.array(), buffer.array().length);
        }
        return numVectors;
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

        public DocsWithFieldSet getDocsWithFieldSet() {
            return flatFieldVectorsWriter.getDocsWithFieldSet();
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + flatFieldVectorsWriter.ramBytesUsed();
        }
    }
}
