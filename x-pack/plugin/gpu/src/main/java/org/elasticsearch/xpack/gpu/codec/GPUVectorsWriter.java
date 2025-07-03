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
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writer for GPU-accelerated vectors.
 */
public class GPUVectorsWriter extends KnnVectorsWriter {
    private static final Logger logger = LogManager.getLogger(GPUVectorsWriter.class);
    // 2 for now based on https://github.com/rapidsai/cuvs/issues/666, but can be increased later
    private static final int MIN_NUM_VECTORS_FOR_GPU_BUILD = 2;

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput gpuIdx;
    private final IndexOutput gpuMeta;
    private final FlatVectorsWriter rawVectorDelegate;
    private final SegmentWriteState segmentWriteState;
    private final CuVSResources cuVSResources;

    @SuppressWarnings("this-escape")
    public GPUVectorsWriter(SegmentWriteState state, FlatVectorsWriter rawVectorDelegate) throws IOException {
        this.cuVSResources = GPUVectorsFormat.cuVSResourcesOrNull();
        if (cuVSResources == null) {
            throw new IllegalArgumentException("GPU based vector search is not supported on this platform or java version");
        }
        this.segmentWriteState = state;
        this.rawVectorDelegate = rawVectorDelegate;
        final String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            GPUVectorsFormat.GPU_META_EXTENSION
        );

        final String gpuIdxFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            GPUVectorsFormat.GPU_IDX_EXTENSION
        );
        boolean success = false;
        try {
            gpuMeta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(
                gpuMeta,
                GPUVectorsFormat.NAME,
                GPUVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            gpuIdx = state.directory.createOutput(gpuIdxFileName, state.context);
            CodecUtil.writeIndexHeader(
                gpuIdx,
                GPUVectorsFormat.NAME,
                GPUVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public final KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        final FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            @SuppressWarnings("unchecked")
            final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) rawVectorDelegate;
            fieldWriters.add(new FieldWriter(fieldInfo, floatWriter));
        }
        return rawVectorDelegate;
    }

    @Override
    public final void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        // TODO: implement the case when sortMap != null

        for (FieldWriter fieldWriter : fieldWriters) {
            // TODO: can we use MemorySegment instead of passing array of vectors
            float[][] vectors = fieldWriter.delegate.getVectors().toArray(float[][]::new);
            long dataOffset = gpuIdx.alignFilePointer(Float.BYTES);
            try {
                buildAndwriteGPUIndex(fieldWriter.fieldInfo.getVectorSimilarityFunction(), vectors);
                long dataLength = gpuIdx.getFilePointer() - dataOffset;
                writeMeta(fieldWriter.fieldInfo, dataOffset, dataLength);
            } catch (IOException e) {
                throw e;
            } catch (Throwable t) {
                throw new IOException("Failed to write GPU index: ", t);
            }
        }
    }

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void buildAndwriteGPUIndex(VectorSimilarityFunction similarityFunction, float[][] vectors) throws Throwable {
        // TODO: should we Lucene HNSW index write here
        if (vectors.length < MIN_NUM_VECTORS_FOR_GPU_BUILD) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skip building carga index; vectors length {} < {}", vectors.length, MIN_NUM_VECTORS_FOR_GPU_BUILD);
            }
            return;
        }

        int dimension = vectors[0].length;
        CagraIndexParams.CuvsDistanceType distanceType = switch (similarityFunction) {
            case EUCLIDEAN -> CagraIndexParams.CuvsDistanceType.L2Expanded;
            case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> CagraIndexParams.CuvsDistanceType.InnerProduct;
            case COSINE -> CagraIndexParams.CuvsDistanceType.CosineExpanded;
        };

        // TODO: expose cagra index params of intermediate graph degree, graph degre, algorithm, NNDescentNumIterations
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
        // serialize index for CPU consumption to hnwslib format
        startTime = System.nanoTime();
        IndexOutput tempCagraHNSW = null;
        boolean success = false;
        try {
            tempCagraHNSW = segmentWriteState.directory.createTempOutput(gpuIdx.getName(), "cagra_hnws_temp", segmentWriteState.context);
            var tempCagraHNSWOutputStream = new IndexOutputOutputStream(tempCagraHNSW);
            index.serializeToHNSW(tempCagraHNSWOutputStream);
            success = true;
            if (logger.isDebugEnabled()) {
                logger.debug("Carga index serialized to hnswlib format in: {} ms", (System.nanoTime() - startTime) / 1_000_000.0);
            }
        } finally {
            index.destroyIndex();
            if (success) {
                IOUtils.close(tempCagraHNSW);
            } else {
                IOUtils.closeWhileHandlingException(tempCagraHNSW);
                if (tempCagraHNSW != null) {
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempCagraHNSW.getName());
                }
            }
        }

        // convert hnswlib format to Lucene HNSW format
        startTime = System.nanoTime();
        success = false;
        IndexInput tempCagraHNSWInput = null;
        try {
            tempCagraHNSWInput = segmentWriteState.directory.openInput(tempCagraHNSW.getName(), segmentWriteState.context);
            // read the metadata from the hnlswlib format
            // some of them are not used in Lucene HNSW format
            tempCagraHNSWInput.readLong(); // offSetLevel0
            long maxElementCount = tempCagraHNSWInput.readLong();
            tempCagraHNSWInput.readLong(); // currElementCount
            long sizeDataPerElement = tempCagraHNSWInput.readLong();
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
            int maxGraphDegree = (int) maxM0;
            int[] connections = new int[maxGraphDegree];
            int dimensionCalculated = (int) ((labelOffset - dataOffset) / Float.BYTES);
            assert (dimension == dimensionCalculated)
                : "Cagra index vector dimension must be: " + dimension + ", got: " + dimensionCalculated;

            // read graph from the cagra_hnswlib index and write it to the Lucene HNSW format
            gpuIdx.writeInt((int) maxElementCount);
            gpuIdx.writeInt((int) maxM0);
            for (int i = 0; i < maxElementCount; i++) {
                // read from the cagra_hnswlib index
                int graphDegree = tempCagraHNSWInput.readInt();
                assert (graphDegree == maxGraphDegree)
                    : "In Cagra graph all nodes must have the same number of connections : " + maxGraphDegree + ", got" + graphDegree;
                for (int j = 0; j < graphDegree; j++) {
                    connections[j] = tempCagraHNSWInput.readInt();
                }
                // Skip over the vector data
                tempCagraHNSWInput.seek(tempCagraHNSWInput.getFilePointer() + dimension * Float.BYTES);
                // Skip over the label/id
                tempCagraHNSWInput.seek(tempCagraHNSWInput.getFilePointer() + Long.BYTES);

                // write graph
                gpuIdx.writeVInt(graphDegree);
                for (int neighbor : connections) {
                    gpuIdx.writeVInt(neighbor);
                }
            }

            success = true;
            if (logger.isDebugEnabled()) {
                logger.debug("cagra_hnws index serialized to Lucene HNSW in: {} ms", (System.nanoTime() - startTime) / 1_000_000.0);
            }
        } finally {
            if (success) {
                IOUtils.close(tempCagraHNSWInput);
            } else {
                IOUtils.closeWhileHandlingException(tempCagraHNSWInput);
            }
            if (tempCagraHNSW != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempCagraHNSW.getName());
            }
        }
    }

    @Override
    public final void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
            FloatVectorValues vectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            // TODO: more efficient way to pass merged vector values to gpuIndex construction
            KnnVectorValues.DocIndexIterator iter = vectorValues.iterator();
            List<float[]> vectorList = new ArrayList<>();
            for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
                vectorList.add(vectorValues.vectorValue(iter.index()));
            }
            float[][] vectors = vectorList.toArray(new float[0][]);

            long dataOffset = gpuIdx.alignFilePointer(Float.BYTES);
            try {
                buildAndwriteGPUIndex(fieldInfo.getVectorSimilarityFunction(), vectors);
                long dataLength = gpuIdx.getFilePointer() - dataOffset;
                writeMeta(fieldInfo, dataOffset, dataLength);
            } catch (IOException e) {
                throw e;
            } catch (Throwable t) {
                throw new IOException("Failed to write GPU index: ", t);
            }
        } else {
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        }
    }

    private void writeMeta(FieldInfo field, long dataOffset, long dataLength) throws IOException {
        gpuMeta.writeInt(field.number);
        gpuMeta.writeInt(field.getVectorEncoding().ordinal());
        gpuMeta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        gpuMeta.writeLong(dataOffset);
        gpuMeta.writeLong(dataLength);
    }

    private static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    @Override
    public final void finish() throws IOException {
        rawVectorDelegate.finish();
        if (gpuMeta != null) {
            // write end of fields marker
            gpuMeta.writeInt(-1);
            CodecUtil.writeFooter(gpuMeta);
        }
        if (gpuIdx != null) {
            CodecUtil.writeFooter(gpuIdx);
        }
    }

    @Override
    public final void close() throws IOException {
        IOUtils.close(rawVectorDelegate, gpuMeta, gpuIdx);
        cuVSResources.close();
    }

    @Override
    public final long ramBytesUsed() {
        return rawVectorDelegate.ramBytesUsed();
    }

    private record FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {}
}
