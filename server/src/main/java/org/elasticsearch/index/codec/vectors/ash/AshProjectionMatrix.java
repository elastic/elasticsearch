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
import org.elasticsearch.simdvec.ESVectorUtil;

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

    private final float[] w;
    private final int originalDim;
    private final int nDims;
    private float[] wT; // lazily computed transposed W, length nDims*originalDim, row-major (nDims x originalDim)

    /**
     * Creates a projection matrix.
     *
     * @param w           the projection matrix in row-major order, length originalDim*nDims
     * @param originalDim number of rows (original vector dimensionality)
     * @param nDims       number of columns (projected dimensionality)
     */
    public AshProjectionMatrix(float[] w, int originalDim, int nDims) {
        if (w.length != originalDim * nDims) {
            throw new IllegalArgumentException("w.length " + w.length + " != originalDim * nDims " + (originalDim * nDims));
        }
        this.w = w;
        this.originalDim = originalDim;
        this.nDims = nDims;
    }

    /**
     * Returns the projection matrix W in row-major order, shape (originalDim, nDims).
     */
    public float[] w() {
        return w;
    }

    /**
     * Returns the transposed projection matrix W^T in row-major order, shape (nDims, originalDim).
     * Row j of wT starts at offset {@code j * originalDim} and is suitable for SIMD dot products.
     * Computed lazily on first access.
     *
     * @return the transposed projection matrix
     */
    public float[] wT() {
        if (wT == null) {
            wT = ESVectorUtil.transposeMatrix(w, originalDim, nDims);
        }
        return wT;
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
        ByteBuffer buffer = ByteBuffer.allocate(w.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(w);
        out.writeBytes(buffer.array(), buffer.capacity());
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
        float[] w = new float[originalDim * nDims];
        ByteBuffer buffer = ByteBuffer.allocate(w.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        in.readBytes(buffer.array(), 0, buffer.capacity());
        buffer.asFloatBuffer().get(w);
        return new AshProjectionMatrix(w, originalDim, nDims);
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
