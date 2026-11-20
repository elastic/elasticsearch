/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

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
 *   [byte] flags: 1 if learned, 2 if seeded
 *   [long, long] id
 *   [int] trainingSampleSize
 *   if seeded:
 *     [long] seed
 *   otherwise:
 *     [float[originalDim * nDims]] WT matrix in row-major order (little-endian)
 * </pre>
 * <p>
 * The id identifies the content of W: a newly trained matrix gets a new random (version 4) UUID, and a matrix that is reused as-is,
 * for instance by a merge from one of its input segments, keeps its id. Two segments with the same id therefore have the
 * same W, so a query projected through one of them can be reused for the other, and readers can share a single in-memory
 * copy. The training sample size is the number of vectors W's subspace was estimated from, 0 if none, so that a matrix
 * can be reused only by segments for which it is as good as one they would learn themselves.
 * <p>
 * A seeded matrix is a random orthonormal projection generated from a seed rather than learned from data, see
 * {@link SeededProjections}. Only its seed is stored, its id is derived from its shape and seed, and its W^T is shared by
 * every segment that uses it. Nothing is learned into it, so it is never inherited at merge.
 */
public final class AshProjectionMatrix {

    private static final byte LEARNED = 1;
    private static final byte SEEDED = 2;

    private final float[] wT;
    private final int originalDim;
    private final int nDims;
    private final boolean learned;
    private final UUID id;
    private final int trainingSampleSize;
    private final boolean seeded;
    // only meaningful if seeded
    private final long seed;

    private AshProjectionMatrix(
        float[] wT,
        int originalDim,
        int nDims,
        boolean learned,
        UUID id,
        int trainingSampleSize,
        boolean seeded,
        long seed
    ) {
        if (wT.length != originalDim * nDims) {
            throw new IllegalArgumentException("wT.length " + wT.length + " != originalDim * nDims " + (originalDim * nDims));
        }
        if (trainingSampleSize < 0) {
            throw new IllegalArgumentException("trainingSampleSize must not be negative, got " + trainingSampleSize);
        }
        this.wT = wT;
        this.originalDim = originalDim;
        this.nDims = nDims;
        this.learned = learned;
        this.id = Objects.requireNonNull(id);
        this.trainingSampleSize = trainingSampleSize;
        this.seeded = seeded;
        this.seed = seed;
    }

    /**
     * Returns a projection matrix stored as its W^T.
     *
     * @param wT          the transposed projection matrix in row-major order, length originalDim*nDims
     * @param originalDim number of rows (original vector dimensionality)
     * @param nDims       number of columns (projected dimensionality)
     * @param learned     whether W may be inherited or warm-started at merge: {@code true} if it was learned via PCA +
     *                    rotation from at least twice as many vectors as projected dimensions; {@code false} otherwise,
     *                    for which merge should learn a full W instead
     * @param id          the identity of W's content, see {@link AshProjectionMatrix}
     * @param trainingSampleSize the number of vectors W's subspace was estimated from, 0 if none
     */
    public static AshProjectionMatrix of(float[] wT, int originalDim, int nDims, boolean learned, UUID id, int trainingSampleSize) {
        return new AshProjectionMatrix(wT, originalDim, nDims, learned, id, trainingSampleSize, false, 0L);
    }

    /**
     * Returns the random orthonormal projection of the given shape generated from {@code seed}, generating it with
     * {@code executor}, or on the calling thread if {@code null}, if it is not cached; the result does not depend on the
     * executor.
     */
    public static AshProjectionMatrix seeded(int originalDim, int nDims, long seed, TaskExecutor executor) {
        return new AshProjectionMatrix(
            SeededProjections.wT(originalDim, nDims, seed, executor),
            originalDim,
            nDims,
            false,
            SeededProjections.id(originalDim, nDims, seed),
            0,
            true,
            seed
        );
    }

    /**
     * Returns whether W was learned (PCA + rotation) from enough vectors to be inherited at merge time, rather than being
     * a random orthonormal projection or learned from too few vectors.
     */
    public boolean isLearned() {
        return learned;
    }

    /** Returns whether W is a random orthonormal projection generated from a seed, which is all that is stored of it. */
    public boolean isSeeded() {
        return seeded;
    }

    /** Returns the identity of W's content. */
    public UUID id() {
        return id;
    }

    /** Returns the number of vectors W's subspace was estimated from, 0 if none. */
    public int trainingSampleSize() {
        return trainingSampleSize;
    }

    /**
     * Returns the transposed projection matrix W^T in row-major order, shape (nDims, originalDim). It may be shared with
     * other matrices, so it must not be modified.
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
        out.writeByte((byte) ((learned ? LEARNED : 0) | (seeded ? SEEDED : 0)));
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeInt(trainingSampleSize);
        if (seeded) {
            out.writeLong(seed);
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocate(wT.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(wT);
        out.writeBytes(buffer.array(), buffer.capacity());
    }

    /**
     * Reads a projection matrix from the given input, with its float W^T, as writers and merges need.
     *
     * @param in the index input to read from
     * @return the deserialized projection matrix
     * @throws IOException if an I/O error occurs
     */
    public static AshProjectionMatrix read(IndexInput in) throws IOException {
        Header header = readHeader(in);
        if (header.seeded()) {
            return seeded(header.originalDim(), header.nDims(), readSeed(in, header), null);
        }
        float[] wT = new float[header.originalDim() * header.nDims()];
        in.readFloats(wT, 0, wT.length);
        return of(wT, header.originalDim(), header.nDims(), header.learned(), header.id(), header.trainingSampleSize());
    }

    /**
     * Reads the int8 projection of queries through a projection matrix from the given input, as searches need, without
     * holding the float W^T in memory.
     *
     * @param in the index input to read from
     * @return the query projection
     * @throws IOException if an I/O error occurs
     */
    public static AshQueryProjection readQueryProjection(IndexInput in) throws IOException {
        Header header = readHeader(in);
        int originalDim = header.originalDim();
        if (header.seeded()) {
            return SeededProjections.queryProjection(originalDim, header.nDims(), readSeed(in, header));
        }
        return AshQueryProjection.quantize(header.id(), originalDim, header.nDims(), (j, row) -> in.readFloats(row, 0, originalDim));
    }

    /**
     * What is stored of a projection matrix before its W^T or seed: enough to decide whether to read the matrix, for
     * instance to choose which input segment's matrix a merge inherits.
     */
    public record Header(int originalDim, int nDims, boolean learned, boolean seeded, UUID id, int trainingSampleSize) {
        /** Returns whether the matrix projects vectors of {@code expectedOriginalDim} dimensions to {@code expectedNDims}. */
        public boolean hasShape(int expectedOriginalDim, int expectedNDims) {
            return originalDim == expectedOriginalDim && nDims == expectedNDims;
        }
    }

    /** Reads the {@link Header} of a projection matrix, leaving the input before its W^T or seed. */
    public static Header readHeader(IndexInput in) throws IOException {
        int originalDim = in.readInt();
        int nDims = in.readInt();
        byte flags = in.readByte();
        if ((flags & ~(LEARNED | SEEDED)) != 0) {
            throw new CorruptIndexException("unknown ASH projection matrix flags [" + flags + "]", in);
        }
        UUID id = new UUID(in.readLong(), in.readLong());
        int trainingSampleSize = in.readInt();
        return new Header(originalDim, nDims, (flags & LEARNED) != 0, (flags & SEEDED) != 0, id, trainingSampleSize);
    }

    private static long readSeed(IndexInput in, Header header) throws IOException {
        long seed = in.readLong();
        // the id is derived from the shape and seed, so a mismatch means the derivation, and maybe the projection, changed
        if (SeededProjections.id(header.originalDim(), header.nDims(), seed).equals(header.id()) == false) {
            throw new CorruptIndexException("ASH seeded projection id [" + header.id() + "] does not match its shape and seed", in);
        }
        return seed;
    }

    /**
     * Returns the byte size of the serialized data.
     *
     * @return total bytes when serialized
     */
    public long byteSize() {
        long header = Integer.BYTES * 2L + Byte.BYTES + Long.BYTES * 2L + Integer.BYTES;
        return header + (seeded ? Long.BYTES : (long) originalDim * nDims * Float.BYTES);
    }
}
