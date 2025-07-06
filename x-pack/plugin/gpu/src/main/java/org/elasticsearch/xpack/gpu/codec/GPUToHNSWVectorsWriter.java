/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSResources;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.LUCENE99_HNSW_META_CODEC_NAME;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.LUCENE99_HNSW_META_EXTENSION;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.LUCENE99_HNSW_VECTOR_INDEX_EXTENSION;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.LUCENE99_VERSION_CURRENT;
import static org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat.MIN_NUM_VECTORS_FOR_GPU_BUILD;

/**
 * Writer that builds a Nvidia Carga Graph on GPU and than writes it into the Lucene99 HNSW format,
 * so that it can be searched on CPU with Lucene99HNSWVectorReader.
 */
final class GPUToHNSWVectorsWriter extends KnnVectorsWriter {
    private static final Logger logger = LogManager.getLogger(GPUToHNSWVectorsWriter.class);
    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(GPUToHNSWVectorsWriter.class);
    private static final int LUCENE99_HNSW_DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final CuVSResources cuVSResources;
    private final SegmentWriteState segmentWriteState;
    private final IndexOutput meta, vectorIndex;
    private final int M;
    private final int beamWidth;
    private final FlatVectorsWriter flatVectorWriter;

    private final List<FieldWriter> fields = new ArrayList<>();
    private boolean finished;

    GPUToHNSWVectorsWriter(CuVSResources cuVSResources, SegmentWriteState state, int M, int beamWidth, FlatVectorsWriter flatVectorWriter)
        throws IOException {
        assert cuVSResources != null : "CuVSResources must not be null";
        this.cuVSResources = cuVSResources;
        this.M = M;
        this.flatVectorWriter = flatVectorWriter;
        this.beamWidth = beamWidth;
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

    private void writeField(FieldWriter fieldWriter) throws IOException {
        float[][] vectors = fieldWriter.flatFieldVectorsWriter.getVectors().toArray(float[][]::new);
        writeFieldInternal(fieldWriter.fieldInfo, vectors);
    }

    private void writeSortingField(FieldWriter fieldData, Sorter.DocMap sortMap) throws IOException {
        // TODO: implement writing sorted field when we can access cagra index through MemorySegment
        // as we need random access to neighbors in the graph.
        throw new UnsupportedOperationException("Writing field with index sorted needs to be implemented.");
    }

    private void writeFieldInternal(FieldInfo fieldInfo, float[][] vectors) throws IOException {
        try {
            long vectorIndexOffset = vectorIndex.getFilePointer();
            int[][] graphLevelNodeOffsets = new int[1][];
            HnswGraph mockGraph;
            if (vectors.length < MIN_NUM_VECTORS_FOR_GPU_BUILD) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Skip building carga index; vectors length {} < {} (min for GPU)",
                        vectors.length,
                        MIN_NUM_VECTORS_FOR_GPU_BUILD
                    );
                }
                mockGraph = writeGraph(vectors, graphLevelNodeOffsets);
            } else {
                String tempCagraHNSWFileName = buildGPUIndex(fieldInfo.getVectorSimilarityFunction(), vectors);
                assert tempCagraHNSWFileName != null : "GPU index should be built for field: " + fieldInfo.name;
                mockGraph = writeGraph(tempCagraHNSWFileName, graphLevelNodeOffsets);
            }
            long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
            writeMeta(fieldInfo, vectorIndexOffset, vectorIndexLength, vectors.length, mockGraph, graphLevelNodeOffsets);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to write GPU index: ", t);
        }
    }

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private String buildGPUIndex(VectorSimilarityFunction similarityFunction, float[][] vectors) throws Throwable {
        CagraIndexParams.CuvsDistanceType distanceType = switch (similarityFunction) {
            case EUCLIDEAN -> CagraIndexParams.CuvsDistanceType.L2Expanded;
            case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> CagraIndexParams.CuvsDistanceType.InnerProduct;
            case COSINE -> CagraIndexParams.CuvsDistanceType.CosineExpanded;
        };

        // TODO: expose cagra index params of intermediate graph degree, graph degree, algorithm, NNDescentNumIterations
        CagraIndexParams params = new CagraIndexParams.Builder().withNumWriterThreads(1) // TODO: how many CPU threads we can use?
            .withCagraGraphBuildAlgo(CagraIndexParams.CagraGraphBuildAlgo.NN_DESCENT)
            .withMetric(distanceType)
            .build();

        // build index on GPU
        long startTime = System.nanoTime();
        var index = CagraIndex.newBuilder(cuVSResources).withDataset(vectors).withIndexParams(params).build();
        if (logger.isDebugEnabled()) {
            logger.debug("Carga index created in: {} ms; #num vectors: {}", (System.nanoTime() - startTime) / 1_000_000.0, vectors.length);
        }

        // TODO: do serialization through MemorySegment instead of a temp file
        // serialize index for CPU consumption to the hnwslib format
        startTime = System.nanoTime();
        IndexOutput tempCagraHNSW = null;
        boolean success = false;
        try {
            tempCagraHNSW = segmentWriteState.directory.createTempOutput(
                vectorIndex.getName(),
                "cagra_hnws_temp",
                segmentWriteState.context
            );
            var tempCagraHNSWOutputStream = new IndexOutputOutputStream(tempCagraHNSW);
            index.serializeToHNSW(tempCagraHNSWOutputStream);
            if (logger.isDebugEnabled()) {
                logger.debug("Carga index serialized to hnswlib format in: {} ms", (System.nanoTime() - startTime) / 1_000_000.0);
            }
            success = true;
        } finally {
            index.destroyIndex();
            if (success) {
                org.elasticsearch.core.IOUtils.close(tempCagraHNSW);
            } else {
                if (tempCagraHNSW != null) {
                    IOUtils.closeWhileHandlingException(tempCagraHNSW);
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempCagraHNSW.getName());
                }
            }
        }
        return tempCagraHNSW.getName();
    }

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private HnswGraph writeGraph(String tempCagraHNSWFileName, int[][] levelNodeOffsets) throws IOException {
        long startTime = System.nanoTime();
        boolean success = false;
        IndexInput tempCagraHNSWInput = null;
        int maxElementCount;
        int maxGraphDegree;

        try {
            tempCagraHNSWInput = segmentWriteState.directory.openInput(tempCagraHNSWFileName, segmentWriteState.context);
            // read the metadata from the hnlswlib format;
            // some of them are not used in the Lucene HNSW format
            tempCagraHNSWInput.readLong(); // offSetLevel0
            maxElementCount = (int) tempCagraHNSWInput.readLong();
            tempCagraHNSWInput.readLong(); // currElementCount
            tempCagraHNSWInput.readLong(); // sizeDataPerElement
            long labelOffset = tempCagraHNSWInput.readLong();
            long dataOffset = tempCagraHNSWInput.readLong();
            int maxLevel = tempCagraHNSWInput.readInt();
            tempCagraHNSWInput.readInt(); // entryPointNode
            tempCagraHNSWInput.readLong(); // maxM
            long maxM0 = tempCagraHNSWInput.readLong(); // number of graph connections
            tempCagraHNSWInput.readLong(); // M
            tempCagraHNSWInput.readLong(); // mult
            tempCagraHNSWInput.readLong(); // efConstruction

            assert (maxLevel == 1) : "Cagra index is flat, maxLevel must be: 1, got: " + maxLevel;
            maxGraphDegree = (int) maxM0;
            int[] neighbors = new int[maxGraphDegree];
            int dimension = (int) ((labelOffset - dataOffset) / Float.BYTES);
            // assert (dimension == dimensionCalculated)
            // : "Cagra index vector dimension must be: " + dimension + ", got: " + dimensionCalculated;

            levelNodeOffsets[0] = new int[maxElementCount];

            // read graph from the cagra_hnswlib index and write it to the Lucene vectorIndex file
            int[] scratch = new int[maxGraphDegree];
            for (int node = 0; node < maxElementCount; node++) {
                // read from the cagra_hnswlib index
                int nodeDegree = tempCagraHNSWInput.readInt();
                assert (nodeDegree == maxGraphDegree)
                    : "In Cagra graph all nodes must have the same number of connections : " + maxGraphDegree + ", got" + nodeDegree;
                for (int i = 0; i < nodeDegree; i++) {
                    neighbors[i] = tempCagraHNSWInput.readInt();
                }
                // Skip over the vector data
                tempCagraHNSWInput.seek(tempCagraHNSWInput.getFilePointer() + dimension * Float.BYTES);
                // Skip over the label/id
                tempCagraHNSWInput.seek(tempCagraHNSWInput.getFilePointer() + Long.BYTES);

                // write to the Lucene vectorIndex file
                long offsetStart = vectorIndex.getFilePointer();
                Arrays.sort(neighbors);
                int actualSize = 0;
                scratch[actualSize++] = neighbors[0];
                for (int i = 1; i < nodeDegree; i++) {
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
            success = true;
        } finally {
            if (success) {
                IOUtils.close(tempCagraHNSWInput);
            } else {
                IOUtils.closeWhileHandlingException(tempCagraHNSWInput);
            }
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempCagraHNSWFileName);
        }
        return createMockGraph(maxElementCount, maxGraphDegree);
    }

    // create a graph where every node is connected to every other node
    private HnswGraph writeGraph(float[][] vectors, int[][] levelNodeOffsets) throws IOException {
        if (vectors.length == 0) {
            return null;
        }
        int elementCount = vectors.length;
        int nodeDegree = vectors.length - 1;
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
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        flatVectorWriter.mergeOneField(fieldInfo, mergeState);
        FloatVectorValues vectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        // TODO: more efficient way to pass merged vector values to gpuIndex construction
        KnnVectorValues.DocIndexIterator iter = vectorValues.iterator();
        List<float[]> vectorList = new ArrayList<>();
        for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
            vectorList.add(vectorValues.vectorValue(iter.index()));
        }
        float[][] vectors = vectorList.toArray(new float[0][]);

        writeFieldInternal(fieldInfo, vectors);
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
