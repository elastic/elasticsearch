/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

public class QuantizedCentroids implements QuantizedVectorValues {
    private final CentroidSupplier supplier;
    private final OptimizedScalarQuantizer quantizer;
    private final byte[] quantizedVector;
    private final int[] quantizedVectorScratch;
    private final float[] floatVectorScratch;
    private OptimizedScalarQuantizer.QuantizationResult corrections;
    private final float[] centroid;
    private int currOrd = -1;
    private IntToIntFunction ordTransformer = i -> i;
    int size;

    public QuantizedCentroids(CentroidSupplier supplier, int dimension, OptimizedScalarQuantizer quantizer, float[] centroid) {
        this.supplier = supplier;
        this.quantizer = quantizer;
        this.quantizedVector = new byte[dimension];
        this.floatVectorScratch = new float[dimension];
        this.quantizedVectorScratch = new int[dimension];
        this.centroid = centroid;
        size = supplier.size();
    }

    @Override
    public int count() {
        return size;
    }

    public void reset(IntToIntFunction ordTransformer, int size) {
        this.ordTransformer = ordTransformer;
        this.currOrd = -1;
        this.size = size;
        this.corrections = null;
    }

    @Override
    public byte[] next() throws IOException {
        if (currOrd >= count() - 1) {
            throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count());
        }
        currOrd++;
        float[] vector = supplier.centroid(ordTransformer.apply(currOrd));
        corrections = quantizer.scalarQuantize(vector, floatVectorScratch, quantizedVectorScratch, (byte) 7, centroid);
        for (int i = 0; i < quantizedVectorScratch.length; i++) {
            quantizedVector[i] = (byte) quantizedVectorScratch[i];
        }
        return quantizedVector;
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
        return corrections;
    }
}
