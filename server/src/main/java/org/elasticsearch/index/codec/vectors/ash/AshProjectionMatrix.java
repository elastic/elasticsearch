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
 * Serialization for the ASH projection matrix W in transposed form.
 * Stored in the preconditioner slot of the {@code .cenivf} file.
 * <p>
 * Centroids are not stored here — in the IVF context, each posting list implicitly
 * defines its own centroid, so no separate centroid storage is needed.
 * <p>
 * Format:
 * <pre>
 *   [int] originalDim (number of rows in W)
 *   [int] nDims (number of columns in W, i.e. projected dimensions)
 *   [float[originalDim * nDims]] WT matrix in row-major order (little-endian)
 * </pre>
 */
public final class AshProjectionMatrix {

    private final float[] wT;
    private final int originalDim;
    private final int nDims;

    /**
     * Creates a projection matrix.
     *
     * @param wT          the transposed projection matrix in row-major order, length originalDim*nDims
     * @param originalDim number of rows (original vector dimensionality)
     * @param nDims       number of columns (projected dimensionality)
     */
    public AshProjectionMatrix(float[] wT, int originalDim, int nDims) {
        if (wT.length != originalDim * nDims) {
            throw new IllegalArgumentException("wT.length " + wT.length + " != originalDim * nDims " + (originalDim * nDims));
        }
        this.wT = wT;
        this.originalDim = originalDim;
        this.nDims = nDims;
    }

    /**
     * Returns the transposed projection matrix W^T in row-major order, shape (nDims, originalDim).
     *
     * @return the transposed projection matrix
     */
    public float[] wT() {
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
        ByteBuffer buffer = ByteBuffer.allocate(wT.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(wT);
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
        float[] wT = new float[originalDim * nDims];
        ByteBuffer buffer = ByteBuffer.allocate(wT.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        in.readBytes(buffer.array(), 0, buffer.capacity());
        buffer.asFloatBuffer().get(wT);
        return new AshProjectionMatrix(wT, originalDim, nDims);
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
