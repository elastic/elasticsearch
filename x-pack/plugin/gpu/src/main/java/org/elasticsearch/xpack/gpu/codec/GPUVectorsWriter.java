/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;

/**
 * Writer for GPU-accelerated vectors.
 */
public class GPUVectorsWriter extends KnnVectorsWriter {

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput gpuIdx;
    private final IndexOutput gpuMeta;
    private final FlatVectorsWriter rawVectorDelegate;
    private final SegmentWriteState segmentWriteState;

    @SuppressWarnings("this-escape")
    public GPUVectorsWriter(SegmentWriteState state, FlatVectorsWriter rawVectorDelegate) throws IOException {
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
        for (FieldWriter fieldWriter : fieldWriters) {
            // TODO: Implement GPU-specific vector merging instead of bogus implementation
            long dataOffset = gpuIdx.alignFilePointer(Float.BYTES);
            var vectors = fieldWriter.delegate.getVectors();
            for (int i = 0; i < vectors.size(); i++) {
                gpuIdx.writeVInt(0);
            }
            long dataLength = gpuIdx.getFilePointer() - dataOffset;
            writeMeta(fieldWriter.fieldInfo, dataOffset, dataLength);
        }
    }

    @Override
    public final void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
            // TODO: Implement GPU-specific vector merging instead of bogus implementation
            FloatVectorValues floatVectorValues = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            long dataOffset = gpuIdx.alignFilePointer(Float.BYTES);
            for (int i = 0; i < floatVectorValues.size(); i++) {
                gpuIdx.writeVInt(0);
            }
            long dataLength = gpuIdx.getFilePointer() - dataOffset;
            writeMeta(fieldInfo, dataOffset, dataLength);
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
    }

    @Override
    public final long ramBytesUsed() {
        return rawVectorDelegate.ramBytesUsed();
    }

    private record FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {}
}
