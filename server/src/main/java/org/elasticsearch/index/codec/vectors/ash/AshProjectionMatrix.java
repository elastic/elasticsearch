/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serialization for the ASH projection matrix W. Stored in the
 * preconditioner slot of the {@code .cenivf} file.
 * <p>
 * Centroids are not stored here — in the IVF context, each posting list implicitly
 * defines its own centroid, so no separate centroid storage is needed.
 * <p>
 * Format:
 * <pre>
 *   [int] originalDim (number of rows in W)
 *   [int] nDims (number of columns in W, i.e. projected dimensions)
 *   [float[originalDim * nDims]] W matrix in row-major order (little-endian)
 * </pre>
 */
public final class AshProjectionMatrix {

    private final float[][] w;
    private final int originalDim;
    private final int nDims;
    private float[][] wT; // lazily computed transposed W (nDims x originalDim) for SIMD dot products

    /**
     * Creates a projection matrix.
     *
     * @param w the projection matrix, shape (originalDim, nDims)
     */
    public AshProjectionMatrix(float[][] w) {
        this.w = w;
        this.originalDim = w.length;
        this.nDims = w.length > 0 ? w[0].length : 0;
    }

    /**
     * Returns the projection matrix W, shape (originalDim, nDims).
     */
    public float[][] w() {
        return w;
    }

    /**
     * Returns the transposed projection matrix W^T (nDims x originalDim).
     * Each row of wT is a contiguous float array suitable for SIMD dot products.
     * Computed lazily on first access.
     *
     * @return the transposed projection matrix
     */
    public float[][] wT() {
        if (wT == null) {
            wT = transposeMatrix(w);
        }
        return wT;
    }

    /**
     * Transposes a matrix from (rows x cols) to (cols x rows).
     */
    private static float[][] transposeMatrix(float[][] m) {
        int rows = m.length;
        int cols = m[0].length;
        float[][] t = new float[cols][rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                t[j][i] = m[i][j];
            }
        }
        return t;
    }

    /**
     * Returns the number of rows in W (original vector dimensionality).
     */
    public int originalDim() {
        return originalDim;
    }

    /**
     * Returns the number of columns in W (projected dimensionality).
     */
    public int nDims() {
        return nDims;
    }

    /**
     * Writes the projection matrix to the given output.
     *
     * @param out the index output to write to
     * @throws IOException if an I/O error occurs
     */
    public void write(IndexOutput out) throws IOException {
        out.writeInt(originalDim);
        out.writeInt(nDims);
        ByteBuffer buffer = ByteBuffer.allocate(nDims * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < originalDim; i++) {
            buffer.asFloatBuffer().put(w[i]);
            out.writeBytes(buffer.array(), nDims * Float.BYTES);
        }
    }

    /**
     * Reads a projection matrix from the given input.
     *
     * @param in the index input to read from
     * @return the deserialized projection matrix
     * @throws IOException if an I/O error occurs
     */
    public static AshProjectionMatrix read(IndexInput in) throws IOException {
        int originalDim = in.readInt();
        int nDims = in.readInt();
        float[][] w = new float[originalDim][nDims];
        byte[] rowBytes = new byte[nDims * Float.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(rowBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < originalDim; i++) {
            in.readBytes(rowBytes, 0, nDims * Float.BYTES);
            buffer.asFloatBuffer().get(w[i]);
        }
        return new AshProjectionMatrix(w);
    }

    /**
     * Returns the byte size of the serialized data.
     *
     * @return total bytes when serialized
     */
    public long byteSize() {
        return Integer.BYTES * 2L + (long) originalDim * nDims * Float.BYTES;
    }
}
