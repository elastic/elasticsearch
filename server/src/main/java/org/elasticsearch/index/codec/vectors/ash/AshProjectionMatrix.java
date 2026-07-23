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
 * Serialization for the ASH projection matrix W and ASH centroids. Stored in the
 * preconditioner slot of the {@code .cenivf} file.
 * <p>
 * Format:
 * <pre>
 *   [int] originalDim (number of rows in W)
 *   [int] nDims (number of columns in W, i.e. projected dimensions)
 *   [int] nAshClusters (number of ASH centroids; 0 for legacy format without centroids)
 *   [float[originalDim * nDims]] W matrix in row-major order (little-endian)
 *   [float[nAshClusters * originalDim]] ASH centroids in row-major order (little-endian)
 * </pre>
 */
public final class AshProjectionMatrix {

    private final float[][] w;
    private final float[][] ashCentroids; // may be null for legacy
    private final int originalDim;
    private final int nDims;
    private float[][] wT; // lazily computed transposed W (nDims x originalDim) for SIMD dot products

    /**
     * Creates a projection matrix without ASH centroids.
     *
     * @param w the projection matrix, shape (originalDim, nDims)
     */
    public AshProjectionMatrix(float[][] w) {
        this(w, null);
    }

    /**
     * Creates a projection matrix with ASH centroids.
     *
     * @param w the projection matrix, shape (originalDim, nDims)
     * @param ashCentroids the ASH centroids, shape (nClusters, originalDim), or {@code null}
     */
    public AshProjectionMatrix(float[][] w, float[][] ashCentroids) {
        this.w = w;
        this.ashCentroids = ashCentroids;
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
            wT = AsymmetricHashingQuantizer.transposeW(w);
        }
        return wT;
    }

    /**
     * Returns the ASH centroids, shape (nClusters, originalDim), or {@code null} if not present.
     */
    public float[][] ashCentroids() {
        return ashCentroids;
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
     * Writes the projection matrix and ASH centroids to the given output.
     *
     * @param out the index output to write to
     * @throws IOException if an I/O error occurs
     */
    public void write(IndexOutput out) throws IOException {
        out.writeInt(originalDim);
        out.writeInt(nDims);
        int nAshClusters = ashCentroids != null ? ashCentroids.length : 0;
        out.writeInt(nAshClusters);
        ByteBuffer buffer = ByteBuffer.allocate(Math.max(nDims, originalDim) * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        // Write W
        for (int i = 0; i < originalDim; i++) {
            buffer.clear();
            buffer.limit(nDims * Float.BYTES);
            buffer.asFloatBuffer().put(w[i]);
            out.writeBytes(buffer.array(), nDims * Float.BYTES);
        }
        // Write ASH centroids
        if (nAshClusters > 0) {
            for (int c = 0; c < nAshClusters; c++) {
                buffer.clear();
                buffer.limit(originalDim * Float.BYTES);
                buffer.asFloatBuffer().put(ashCentroids[c]);
                out.writeBytes(buffer.array(), originalDim * Float.BYTES);
            }
        }
    }

    /**
     * Reads a projection matrix and ASH centroids from the given input.
     *
     * @param in the index input to read from
     * @return the deserialized projection matrix
     * @throws IOException if an I/O error occurs
     */
    public static AshProjectionMatrix read(IndexInput in) throws IOException {
        int originalDim = in.readInt();
        int nDims = in.readInt();
        int nAshClusters = in.readInt();
        float[][] w = new float[originalDim][nDims];
        byte[] rowBytes = new byte[Math.max(nDims, originalDim) * Float.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(rowBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < originalDim; i++) {
            in.readBytes(rowBytes, 0, nDims * Float.BYTES);
            buffer.clear();
            buffer.limit(nDims * Float.BYTES);
            buffer.asFloatBuffer().get(w[i]);
        }
        float[][] ashCentroids = null;
        if (nAshClusters > 0) {
            ashCentroids = new float[nAshClusters][originalDim];
            for (int c = 0; c < nAshClusters; c++) {
                in.readBytes(rowBytes, 0, originalDim * Float.BYTES);
                buffer.clear();
                buffer.limit(originalDim * Float.BYTES);
                buffer.asFloatBuffer().get(ashCentroids[c]);
            }
        }
        return new AshProjectionMatrix(w, ashCentroids);
    }

    /**
     * Returns the byte size of the serialized data.
     *
     * @return total bytes when serialized
     */
    public long byteSize() {
        long size = Integer.BYTES * 3 + (long) originalDim * nDims * Float.BYTES;
        if (ashCentroids != null) {
            size += (long) ashCentroids.length * originalDim * Float.BYTES;
        }
        return size;
    }
}
