/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A lookup table that stores vertex coordinates, each assigned a sequential ordinal.
 * The table starts with the vertices of the original geometry in geometry order (ring by ring,
 * without closing vertices), so the geometry can be reconstructed by reading it sequentially,
 * guided only by the part and ring counts in the connectivity section. The triangle tree references
 * vertices by ordinal instead of storing coordinate values directly, which deduplicates shared
 * vertices (a vertex typically appears in ~6 adjacent triangles). Vertices only needed by the tree
 * (e.g. from dateline normalization) are appended after the geometry vertices.
 *
 * <p>Coordinates are stored as unsigned offsets from the minimum of the document's {@link Extent},
 * using a fixed number of bytes per axis (0 to 4) chosen per document from the largest offset. A small
 * shape therefore needs only one or two bytes per coordinate, while entries stay fixed-size, enabling
 * O(1) random access by ordinal during triangle tree traversal. If any vertex lies below the extent
 * minimum on an axis (possible for geometry vertices that the tessellation drops), that axis falls back
 * to absolute 4-byte values. The read path avoids allocating coordinate arrays by reading directly from
 * the underlying doc-value byte array.
 *
 * <p>Layout: {@code numVertices(VInt) | layout(byte, only if numVertices > 0) | vertices}, where the
 * layout byte holds the x width in bits 0-2, the y width in bits 3-5, and the absolute flags for x and y
 * in bits 6 and 7. Each vertex is the x offset followed by the y offset, big-endian.
 */
public class VertexLookupTable {

    private static final int WIDTH_MASK = 0x7;
    private static final int Y_WIDTH_SHIFT = 3;
    private static final int ABSOLUTE_X = 1 << 6;
    private static final int ABSOLUTE_Y = 1 << 7;

    private final byte[] bytes;
    private final int dataOffset;
    private final int numVertices;
    private final int baseX;
    private final int baseY;
    private final int widthX;
    private final int widthY;
    private final int stride;

    private VertexLookupTable(byte[] bytes, int dataOffset, int numVertices, int baseX, int baseY, int widthX, int widthY) {
        this.bytes = bytes;
        this.dataOffset = dataOffset;
        this.numVertices = numVertices;
        this.baseX = baseX;
        this.baseY = baseY;
        this.widthX = widthX;
        this.widthY = widthY;
        this.stride = widthX + widthY;
    }

    /** Returns the encoded x-coordinate for the given vertex ordinal. */
    public int getX(int ordinal) {
        return baseX + readUnsigned(bytes, dataOffset + ordinal * stride, widthX);
    }

    /** Returns the encoded y-coordinate for the given vertex ordinal. */
    public int getY(int ordinal) {
        return baseY + readUnsigned(bytes, dataOffset + ordinal * stride + widthX, widthY);
    }

    /** Returns the number of vertices in the table. */
    public int size() {
        return numVertices;
    }

    /**
     * Reads a vertex table from the input stream without allocating coordinate arrays.
     * The returned table reads directly from the underlying byte array on each {@link #getX}/{@link #getY} call.
     *
     * @param input the stream positioned at the start of the vertex table
     * @param bytes the underlying byte array backing the stream (typically {@code BytesRef.bytes})
     * @param extent the extent of the same doc value, which the coordinates are relative to
     */
    public static VertexLookupTable readFrom(ByteArrayStreamInput input, byte[] bytes, Extent extent) throws IOException {
        int size = input.readVInt();
        if (size == 0) {
            return new VertexLookupTable(bytes, input.getPosition(), 0, 0, 0, 0, 0);
        }
        int layout = input.readByte() & 0xFF;
        int widthX = layout & WIDTH_MASK;
        int widthY = (layout >> Y_WIDTH_SHIFT) & WIDTH_MASK;
        int baseX = (layout & ABSOLUTE_X) != 0 ? 0 : extent.minX();
        int baseY = (layout & ABSOLUTE_Y) != 0 ? 0 : extent.minY();
        int dataOffset = input.getPosition();
        input.skipBytes((long) size * (widthX + widthY));
        return new VertexLookupTable(bytes, dataOffset, size, baseX, baseY, widthX, widthY);
    }

    /**
     * Reads a big-endian unsigned value of {@code width} bytes. A 4-byte value may exceed {@link Integer#MAX_VALUE};
     * it is returned wrapped, which is still correct once added to the base with int arithmetic.
     */
    private static int readUnsigned(byte[] bytes, int offset, int width) {
        return switch (width) {
            case 0 -> 0;
            case 1 -> bytes[offset] & 0xFF;
            case 2 -> ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
            case 3 -> ((bytes[offset] & 0xFF) << 16) | ((bytes[offset + 1] & 0xFF) << 8) | (bytes[offset + 2] & 0xFF);
            case 4 -> ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
            default -> throw new IllegalStateException("invalid vertex coordinate width [" + width + "]");
        };
    }

    private static void writeUnsigned(byte[] bytes, int offset, int width, int value) {
        for (int i = width - 1; i >= 0; i--) {
            bytes[offset + i] = (byte) value;
            value >>>= 8;
        }
    }

    /** Creates a new builder for constructing a vertex lookup table from triangle vertices. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder that collects vertices and assigns sequential ordinals.
     * Uses a hash map keyed on the packed (x, y) coordinate pair so that the triangle tree can find
     * the ordinal of an existing vertex. The byte width of each axis is only known once all vertices
     * and the extent are known, so coordinates are buffered and packed in {@link #writeTo}.
     *
     * <p>The original geometry vertices are added first with {@link #appendVertex}, in geometry order and
     * including repeated coordinates, so the table can be read back sequentially to reconstruct the geometry.
     * The triangle tree then resolves its vertices with {@link #addVertex}, which only appends vertices that
     * are not already present (e.g. those created by dateline normalization).
     */
    public static class Builder {
        private final LongIntHashMap coordToOrdinal = new LongIntHashMap(64);
        private int nextOrdinal = 0;
        private int[] xs = new int[64];
        private int[] ys = new int[64];

        /**
         * Appends a vertex at the next ordinal, even if the same coordinates were already added.
         * The first ordinal for each coordinate is the one returned by later {@link #addVertex} calls.
         */
        public int appendVertex(int x, int y) {
            int ordinal = append(x, y);
            coordToOrdinal.putIfAbsent(packCoordinates(x, y), ordinal);
            return ordinal;
        }

        /**
         * Adds a vertex to the table. If the vertex already exists, returns its existing ordinal.
         * Otherwise assigns a new sequential ordinal and returns it.
         */
        public int addVertex(int x, int y) {
            long key = packCoordinates(x, y);
            int index = coordToOrdinal.indexOf(key);
            if (coordToOrdinal.indexExists(index)) {
                return coordToOrdinal.indexGet(index);
            }
            int ordinal = append(x, y);
            coordToOrdinal.indexInsert(index, key, ordinal);
            return ordinal;
        }

        private int append(int x, int y) {
            int ordinal = nextOrdinal++;
            if (ordinal == xs.length) {
                xs = Arrays.copyOf(xs, xs.length * 2);
                ys = Arrays.copyOf(ys, ys.length * 2);
            }
            xs[ordinal] = x;
            ys[ordinal] = y;
            return ordinal;
        }

        /** Returns the number of vertices added so far. */
        public int size() {
            return nextOrdinal;
        }

        /**
         * Serializes the vertex table, with coordinates relative to the minimum of {@code extent},
         * which must be the extent written to the same doc value.
         */
        public void writeTo(StreamOutput out, Extent extent) throws IOException {
            out.writeVInt(nextOrdinal);
            if (nextOrdinal == 0) {
                return;
            }
            boolean absoluteX = min(xs, nextOrdinal) < extent.minX();
            boolean absoluteY = min(ys, nextOrdinal) < extent.minY();
            int baseX = absoluteX ? 0 : extent.minX();
            int baseY = absoluteY ? 0 : extent.minY();
            int widthX = absoluteX ? Integer.BYTES : byteWidth(xs, nextOrdinal, baseX);
            int widthY = absoluteY ? Integer.BYTES : byteWidth(ys, nextOrdinal, baseY);
            out.writeByte((byte) (widthX | (widthY << Y_WIDTH_SHIFT) | (absoluteX ? ABSOLUTE_X : 0) | (absoluteY ? ABSOLUTE_Y : 0)));
            int stride = widthX + widthY;
            byte[] packed = new byte[nextOrdinal * stride];
            for (int i = 0; i < nextOrdinal; i++) {
                writeUnsigned(packed, i * stride, widthX, xs[i] - baseX);
                writeUnsigned(packed, i * stride + widthX, widthY, ys[i] - baseY);
            }
            out.writeBytes(packed, 0, packed.length);
        }

        private static int min(int[] values, int size) {
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < size; i++) {
                min = Math.min(min, values[i]);
            }
            return min;
        }

        /** The number of bytes needed for the largest unsigned offset of {@code values} from {@code base}. */
        private static int byteWidth(int[] values, int size, int base) {
            long maxOffset = 0;
            for (int i = 0; i < size; i++) {
                maxOffset = Math.max(maxOffset, (long) values[i] - base);
            }
            return (Long.SIZE - Long.numberOfLeadingZeros(maxOffset) + 7) / 8;
        }

        private static long packCoordinates(int x, int y) {
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
    }
}
