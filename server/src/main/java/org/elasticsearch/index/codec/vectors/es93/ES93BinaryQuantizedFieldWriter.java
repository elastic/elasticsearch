/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.elasticsearch.index.codec.vectors.BinaryQuantizedVectorsFieldWriter;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * BQ field writer that wraps an {@link ES93FlatFieldVectorsWriter}.
 * Normalization is delegated to the underlying off-heap vector store
 * so that mutations happen in place on MemorySegments.
 */
class ES93BinaryQuantizedFieldWriter extends BinaryQuantizedVectorsFieldWriter {

    private static final long SHALLOW_SIZE = shallowSizeOfInstance(ES93BinaryQuantizedFieldWriter.class);

    private final FieldInfo fieldInfo;
    private final ES93FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;
    private final FloatArrayList magnitudes = new FloatArrayList();
    private boolean finished;

    ES93BinaryQuantizedFieldWriter(FieldInfo fieldInfo, ES93FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
        this.fieldInfo = fieldInfo;
        this.flatFieldVectorsWriter = flatFieldVectorsWriter;
        this.dimensionSums = new float[fieldInfo.getVectorDimension()];
    }

    @Override
    public FieldInfo fieldInfo() {
        return fieldInfo;
    }

    @Override
    public List<float[]> getVectors() {
        return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public void normalizeVectors() {
        flatFieldVectorsWriter.normalizeByMagnitudes(magnitudes.buffer, 0, magnitudes.size());
    }

    @Override
    public int getVectorCount() {
        return flatFieldVectorsWriter.getVectors().size();
    }

    @Override
    public float[] dimensionSums() {
        return dimensionSums;
    }

    @Override
    public float[] getOnHeapVector(int i) {
        return flatFieldVectorsWriter.getVectors().get(i);
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
        return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
        flatFieldVectorsWriter.addValue(docID, vectorValue);
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
            float dp = ESVectorUtil.dotProduct(vectorValue, vectorValue);
            float divisor = (float) Math.sqrt(dp);
            magnitudes.add(divisor);
            for (int i = 0; i < vectorValue.length; i++) {
                dimensionSums[i] += (vectorValue[i] / divisor);
            }
        } else {
            for (int i = 0; i < vectorValue.length; i++) {
                dimensionSums[i] += vectorValue[i];
            }
        }
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            return;
        }
        assert flatFieldVectorsWriter.isFinished();
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += flatFieldVectorsWriter.ramBytesUsed();
        size += magnitudes.ramBytesUsed();
        return size;
    }
}
