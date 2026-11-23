/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Objects;
import java.util.UUID;

/**
 * The part of an {@link AshProjectionMatrix} that searches need: W^T quantized to int8, each row (one projected dimension)
 * with its own scale, which is all {@link #projectQuery} uses. A reader keeps this rather than the float W^T, which only
 * writers and merges need, so a segment's projection costs one byte per entry of W on the heap rather than four.
 */
public final class AshQueryProjection {

    private final UUID id;
    private final int originalDim;
    private final int nDims;
    // the quantized rows of W^T, row-major (nDims x originalDim)
    private final byte[] values;
    // the scale that maps each quantized row back to W^T
    private final float[] rowScales;

    AshQueryProjection(UUID id, int originalDim, int nDims, byte[] values, float[] rowScales) {
        if (values.length != originalDim * nDims || rowScales.length != nDims) {
            throw new IllegalArgumentException("quantized W^T does not have the shape [" + nDims + "x" + originalDim + "]");
        }
        this.id = Objects.requireNonNull(id);
        this.originalDim = originalDim;
        this.nDims = nDims;
        this.values = values;
        this.rowScales = rowScales;
    }

    /** Quantizes the given W^T, row-major (nDims x originalDim). */
    static AshQueryProjection quantize(UUID id, float[] wT, int originalDim, int nDims) {
        return quantize(id, originalDim, nDims, (j, row) -> System.arraycopy(wT, j * originalDim, row, 0, originalDim));
    }

    /**
     * Quantizes W^T row by row, as {@code readRow} supplies its rows in order, so that the float W^T need not be held in
     * memory: {@code readRow} fills the given buffer with the row of the given index.
     */
    static <E extends Exception> AshQueryProjection quantize(
        UUID id,
        int originalDim,
        int nDims,
        CheckedBiConsumer<Integer, float[], E> readRow
    ) throws E {
        byte[] values = new byte[originalDim * nDims];
        float[] rowScales = new float[nDims];
        float[] row = new float[originalDim];
        for (int j = 0; j < nDims; j++) {
            readRow.accept(j, row);
            rowScales[j] = AshUtils.quantizeInt8(row, 0, originalDim, values, j * originalDim);
        }
        return new AshQueryProjection(id, originalDim, nDims, values, rowScales);
    }

    /** Returns the identity of W's content, see {@link AshProjectionMatrix}. */
    public UUID id() {
        return id;
    }

    /** Returns the number of rows in W (original vector dimensionality). */
    public int originalDim() {
        return originalDim;
    }

    /** Returns the number of columns in W (projected dimensionality). */
    public int nDims() {
        return nDims;
    }

    /**
     * Projects {@code query} into the latent space, as {@code W^T @ query}, using int8 arithmetic.
     * The query is not centered, so the result can be used by both the float and the integer scorer.
     * Both scorers use the absolute values of the projection, so the query scale and the row scales
     * of W^T are applied to the integer result.
     *
     * @param query the raw query vector, length originalDim
     * @return the projected query, length nDims
     */
    public float[] projectQuery(float[] query) {
        if (query.length != originalDim) {
            throw new IllegalArgumentException("query.length " + query.length + " != originalDim " + originalDim);
        }
        byte[] queryQuantized = new byte[originalDim];
        float queryScale = AshUtils.quantizeInt8(query, 0, originalDim, queryQuantized, 0);

        int[] products = new int[nDims];
        ESVectorUtil.matrixVectorMultiply(values, nDims, originalDim, queryQuantized, products);

        float[] projected = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            projected[j] = products[j] * queryScale * rowScales[j];
        }
        return projected;
    }
}
