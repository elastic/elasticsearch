/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A lookup table that stores unique vertex coordinates, each assigned a sequential ordinal.
 * Both the triangle tree and the vertex connectivity structure reference vertices by ordinal
 * instead of storing coordinate values directly. This deduplicates shared vertices (a vertex
 * typically appears in ~6 adjacent triangles) and enables compact storage of the connectivity
 * information needed to reconstruct the original geometry.
 *
 * <p>Each entry is fixed-size (4 bytes x + 4 bytes y = 8 bytes), enabling O(1) random access
 * by ordinal during triangle tree traversal. The read path avoids allocating separate coordinate
 * arrays by reading directly from the underlying doc-value byte array.
 */
public class VertexLookupTable {

    private static final int BYTES_PER_VERTEX = 8; // 4 bytes x + 4 bytes y

    private final byte[] bytes;
    private final int dataOffset;
    private final int numVertices;

    private VertexLookupTable(byte[] bytes, int dataOffset, int numVertices) {
        this.bytes = bytes;
        this.dataOffset = dataOffset;
        this.numVertices = numVertices;
    }

    /** Returns the encoded x-coordinate for the given vertex ordinal. */
    public int getX(int ordinal) {
        return readIntBE(bytes, dataOffset + ordinal * BYTES_PER_VERTEX);
    }

    /** Returns the encoded y-coordinate for the given vertex ordinal. */
    public int getY(int ordinal) {
        return readIntBE(bytes, dataOffset + ordinal * BYTES_PER_VERTEX + 4);
    }

    /** Returns the number of unique vertices in the table. */
    public int size() {
        return numVertices;
    }

    /**
     * Reads a vertex table from the input stream without allocating coordinate arrays.
     * The returned table reads directly from the underlying byte array on each {@link #getX}/{@link #getY} call.
     *
     * @param input the stream positioned at the start of the vertex table
     * @param bytes the underlying byte array backing the stream (typically {@code BytesRef.bytes})
     */
    public static VertexLookupTable readFrom(ByteArrayStreamInput input, byte[] bytes) throws IOException {
        int size = input.readVInt();
        int dataOffset = input.getPosition();
        input.skipBytes((long) size * BYTES_PER_VERTEX);
        return new VertexLookupTable(bytes, dataOffset, size);
    }

    /** Reads a big-endian int from the byte array at the given offset, similar to {@code StreamInput.readInt()}. */
    static int readIntBE(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3]
            & 0xFF);
    }

    /** Writes a big-endian int into the byte array at the given offset, similar to {@code StreamOutput.writeInt()}. */
    static void writeIntBE(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) value;
    }

    /** Creates a new builder for constructing a vertex lookup table from triangle vertices. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder that collects unique vertices and assigns sequential ordinals.
     * Uses a hash map keyed on the packed (x, y) coordinate pair to detect duplicates.
     * Vertex data is stored in a single packed byte array (x, y pairs in big-endian format)
     * rather than separate int arrays, matching the on-disk format and enabling efficient
     * bulk writes.
     */
    public static class Builder {
        private final Map<Long, Integer> coordToOrdinal = new HashMap<>();
        private int nextOrdinal = 0;
        private byte[] vertexData = new byte[64 * BYTES_PER_VERTEX];

        /**
         * Adds a vertex to the table. If the vertex already exists, returns its existing ordinal.
         * Otherwise assigns a new sequential ordinal and returns it.
         */
        public int addVertex(int x, int y) {
            long key = packCoordinates(x, y);
            Integer existing = coordToOrdinal.get(key);
            if (existing != null) {
                return existing;
            }
            int ordinal = nextOrdinal++;
            int byteOffset = ordinal * BYTES_PER_VERTEX;
            if (byteOffset + BYTES_PER_VERTEX > vertexData.length) {
                vertexData = Arrays.copyOf(vertexData, vertexData.length * 2);
            }
            writeIntBE(vertexData, byteOffset, x);
            writeIntBE(vertexData, byteOffset + 4, y);
            coordToOrdinal.put(key, ordinal);
            return ordinal;
        }

        /**
         * Looks up the ordinal for a vertex with the given encoded coordinates.
         * Returns -1 if no such vertex exists in the table.
         */
        public int getOrdinal(int x, int y) {
            long key = packCoordinates(x, y);
            Integer ordinal = coordToOrdinal.get(key);
            return ordinal != null ? ordinal : -1;
        }

        /** Returns the number of unique vertices added so far. */
        public int size() {
            return nextOrdinal;
        }

        /** Serializes the vertex table directly to the output stream. */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(nextOrdinal);
            out.writeBytes(vertexData, 0, nextOrdinal * BYTES_PER_VERTEX);
        }

        private static long packCoordinates(int x, int y) {
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
    }
}
