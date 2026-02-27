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
 * <p>On-disk format uses bit-packed extent-relative encoding: coordinates are stored as offsets
 * from the minimum x/y values, using only the bits needed to represent the actual range.
 * This saves bits proportional to how much smaller the geometry's extent is compared to the
 * full 32-bit coordinate range (e.g. a geometry spanning 5° saves ~4 bits/axis = ~1 byte/vertex).
 * On read, the packed data is decoded into int arrays for O(1) random access by ordinal.
 *
 * <p>The writer also computes the size of zigzag-VLong delta encoding between consecutive vertices
 * (which exploits spatial locality from the tessellator) and picks whichever encoding is smaller.
 * A flag byte distinguishes the two formats.
 */
public class VertexLookupTable {

    static final byte ENCODING_BIT_PACKED = 0;
    static final byte ENCODING_DELTA = 1;

    private static final int BYTES_PER_VERTEX = 8; // 4 bytes x + 4 bytes y (used by builder)

    private final int[] xs;
    private final int[] ys;
    private final int numVertices;

    private VertexLookupTable(int[] xs, int[] ys, int numVertices) {
        this.xs = xs;
        this.ys = ys;
        this.numVertices = numVertices;
    }

    /** Returns the encoded x-coordinate for the given vertex ordinal. */
    public int getX(int ordinal) {
        return xs[ordinal];
    }

    /** Returns the encoded y-coordinate for the given vertex ordinal. */
    public int getY(int ordinal) {
        return ys[ordinal];
    }

    /** Returns the number of unique vertices in the table. */
    public int size() {
        return numVertices;
    }

    /**
     * Reads a vertex table, decoding into int arrays for O(1) access.
     * Supports both bit-packed and delta-encoded formats (distinguished by a flag byte).
     */
    public static VertexLookupTable readFrom(ByteArrayStreamInput input) throws IOException {
        int size = input.readVInt();
        if (size == 0) {
            return new VertexLookupTable(new int[0], new int[0], 0);
        }
        byte encoding = input.readByte();
        return switch (encoding) {
            case ENCODING_BIT_PACKED -> readBitPacked(input, size);
            case ENCODING_DELTA -> readDeltaEncoded(input, size);
            default -> throw new IOException("Unknown vertex table encoding: " + encoding);
        };
    }

    private static VertexLookupTable readBitPacked(ByteArrayStreamInput input, int size) throws IOException {
        int minX = input.readInt();
        int minY = input.readInt();
        int bitsPerX = input.readByte() & 0xFF;
        int bitsPerY = input.readByte() & 0xFF;

        int[] xs = new int[size];
        int[] ys = new int[size];

        long buffer = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < size; i++) {
            while (bitsInBuffer < bitsPerX) {
                buffer = (buffer << 8) | (input.readByte() & 0xFFL);
                bitsInBuffer += 8;
            }
            bitsInBuffer -= bitsPerX;
            xs[i] = minX + (int) ((buffer >>> bitsInBuffer) & mask(bitsPerX));

            while (bitsInBuffer < bitsPerY) {
                buffer = (buffer << 8) | (input.readByte() & 0xFFL);
                bitsInBuffer += 8;
            }
            bitsInBuffer -= bitsPerY;
            ys[i] = minY + (int) ((buffer >>> bitsInBuffer) & mask(bitsPerY));
        }
        return new VertexLookupTable(xs, ys, size);
    }

    private static VertexLookupTable readDeltaEncoded(ByteArrayStreamInput input, int size) throws IOException {
        int[] xs = new int[size];
        int[] ys = new int[size];
        int prevX = 0;
        int prevY = 0;
        for (int i = 0; i < size; i++) {
            prevX += (int) input.readZLong();
            prevY += (int) input.readZLong();
            xs[i] = prevX;
            ys[i] = prevY;
        }
        return new VertexLookupTable(xs, ys, size);
    }

    private static long mask(int bits) {
        return bits == 0 ? 0 : (bits >= 64 ? -1L : (1L << bits) - 1);
    }

    /** Creates a new builder for constructing a vertex lookup table from triangle vertices. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder that collects unique vertices and assigns sequential ordinals.
     * Uses a hash map keyed on the packed (x, y) coordinate pair to detect duplicates.
     * On write, both bit-packed and delta-encoded formats are sized; the smaller one is used.
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

        /**
         * Serializes the vertex table, automatically choosing the smaller encoding
         * between bit-packed extent-relative and zigzag-VLong delta encoding.
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(nextOrdinal);
            if (nextOrdinal == 0) {
                return;
            }

            int bitPackedSize = computeBitPackedSize();
            int deltaSize = computeDeltaSize();

            if (bitPackedSize <= deltaSize) {
                out.writeByte(ENCODING_BIT_PACKED);
                writeBitPacked(out);
            } else {
                out.writeByte(ENCODING_DELTA);
                writeDeltaEncoded(out);
            }
        }

        private int computeBitPackedSize() {
            int minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;
            int minY = Integer.MAX_VALUE, maxY = Integer.MIN_VALUE;
            for (int i = 0; i < nextOrdinal; i++) {
                int x = getBuilderX(i);
                int y = getBuilderY(i);
                minX = Math.min(minX, x);
                maxX = Math.max(maxX, x);
                minY = Math.min(minY, y);
                maxY = Math.max(maxY, y);
            }
            int bitsPerX = bitsNeeded(Integer.toUnsignedLong(maxX - minX));
            int bitsPerY = bitsNeeded(Integer.toUnsignedLong(maxY - minY));
            long totalBits = (long) nextOrdinal * (bitsPerX + bitsPerY);
            int totalBytes = (int) ((totalBits + 7) >>> 3);
            return 4 + 4 + 1 + 1 + totalBytes; // minX + minY + bitsPerX + bitsPerY + data
        }

        private int computeDeltaSize() {
            int size = 0;
            int prevX = 0;
            int prevY = 0;
            for (int i = 0; i < nextOrdinal; i++) {
                int x = getBuilderX(i);
                int y = getBuilderY(i);
                size += zigzagVLongSize(x - prevX);
                size += zigzagVLongSize(y - prevY);
                prevX = x;
                prevY = y;
            }
            return size;
        }

        private void writeBitPacked(StreamOutput out) throws IOException {
            int minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;
            int minY = Integer.MAX_VALUE, maxY = Integer.MIN_VALUE;
            for (int i = 0; i < nextOrdinal; i++) {
                int x = getBuilderX(i);
                int y = getBuilderY(i);
                minX = Math.min(minX, x);
                maxX = Math.max(maxX, x);
                minY = Math.min(minY, y);
                maxY = Math.max(maxY, y);
            }

            int bitsPerX = bitsNeeded(Integer.toUnsignedLong(maxX - minX));
            int bitsPerY = bitsNeeded(Integer.toUnsignedLong(maxY - minY));

            out.writeInt(minX);
            out.writeInt(minY);
            out.writeByte((byte) bitsPerX);
            out.writeByte((byte) bitsPerY);

            long buffer = 0;
            int bitsInBuffer = 0;
            for (int i = 0; i < nextOrdinal; i++) {
                int relX = getBuilderX(i) - minX;
                int relY = getBuilderY(i) - minY;

                buffer = (buffer << bitsPerX) | (Integer.toUnsignedLong(relX) & mask(bitsPerX));
                bitsInBuffer += bitsPerX;
                while (bitsInBuffer >= 8) {
                    bitsInBuffer -= 8;
                    out.writeByte((byte) (buffer >>> bitsInBuffer));
                }

                buffer = (buffer << bitsPerY) | (Integer.toUnsignedLong(relY) & mask(bitsPerY));
                bitsInBuffer += bitsPerY;
                while (bitsInBuffer >= 8) {
                    bitsInBuffer -= 8;
                    out.writeByte((byte) (buffer >>> bitsInBuffer));
                }
            }
            if (bitsInBuffer > 0) {
                out.writeByte((byte) (buffer << (8 - bitsInBuffer)));
            }
        }

        private void writeDeltaEncoded(StreamOutput out) throws IOException {
            int prevX = 0;
            int prevY = 0;
            for (int i = 0; i < nextOrdinal; i++) {
                int x = getBuilderX(i);
                int y = getBuilderY(i);
                out.writeZLong(x - prevX);
                out.writeZLong(y - prevY);
                prevX = x;
                prevY = y;
            }
        }

        private int getBuilderX(int ordinal) {
            return readIntBE(vertexData, ordinal * BYTES_PER_VERTEX);
        }

        private int getBuilderY(int ordinal) {
            return readIntBE(vertexData, ordinal * BYTES_PER_VERTEX + 4);
        }

        private static long packCoordinates(int x, int y) {
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
    }

    static int bitsNeeded(long unsignedRange) {
        if (unsignedRange == 0) return 0;
        return 64 - Long.numberOfLeadingZeros(unsignedRange);
    }

    static int zigzagVLongSize(int delta) {
        long zigzag = (((long) delta) << 1) ^ (((long) delta) >> 63);
        return (70 - Long.numberOfLeadingZeros(zigzag | 1L)) / 7;
    }

    /** Reads a big-endian int from the byte array at the given offset. */
    private static int readIntBE(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3]
            & 0xFF);
    }

    /** Writes a big-endian int into the byte array at the given offset. */
    private static void writeIntBE(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) value;
    }
}
