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
import org.elasticsearch.common.util.ByteUtils;

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
 * <p>Vertices are grouped into blocks of {@link #BLOCK_SIZE} consecutive ordinals. Since the table is in
 * geometry order, the vertices of a block are close together even when the whole shape is large, so each
 * block stores its coordinates as bit-packed offsets from the block minimum, using only as many bits per
 * axis as that block needs. A fixed-size directory entry per block holds the block minimum, the bit widths
 * and the position of the block's packed data, so any ordinal is still read in O(1) without decoding its
 * neighbours. The read path avoids allocating coordinate arrays by reading directly from the underlying
 * doc-value byte array.
 *
 * <p>Block minimums are stored as unsigned offsets from the minimum of the document's {@link Extent}, using
 * a fixed number of bytes per axis (0 to 4) chosen per document from the largest offset. If any vertex lies
 * below the extent minimum on an axis (possible for geometry vertices that the tessellation drops), that
 * axis stores absolute 4-byte block minimums instead.
 *
 * <p>Layout: {@code numVertices(VInt) | layout(byte) | offsetWidth(byte, only if more than one block) |
 * directory | packed data}, written only if {@code numVertices > 0}. The layout byte holds the block-minimum
 * byte width for x in bits 0-2 and for y in bits 3-5, and the absolute flags for x and y in bits 6 and 7.
 * Each directory entry is the block minimum x and y, the x and y bit widths (one byte each), and the byte
 * offset of the block's data from the start of the packed data ({@code offsetWidth} bytes, big-endian).
 * Each block's data holds, per vertex, the x offset followed by the y offset, packed most significant bit first.
 */
public class VertexLookupTable {

    static final int BLOCK_SHIFT = 5;
    static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
    private static final int BLOCK_MASK = BLOCK_SIZE - 1;

    private static final int WIDTH_MASK = 0x7;
    private static final int Y_WIDTH_SHIFT = 3;
    private static final int ABSOLUTE_X = 1 << 6;
    private static final int ABSOLUTE_Y = 1 << 7;

    private final byte[] bytes;
    private final int directoryOffset;
    private final int dataOffset;
    private final int numVertices;
    private final int baseX;
    private final int baseY;
    private final int widthX;
    private final int widthY;
    private final int offsetWidth;
    private final int entryStride;
    private final int minXShift;
    private final long minXMask;
    private final int minYShift;
    private final long minYMask;
    private final int offsetShift;
    private final long offsetMask;

    /** The block whose directory entry is cached below, or -1. Ordinals are usually read in x/y pairs from the same block. */
    private int cachedBlock = -1;
    private int blockMinX;
    private int blockMinY;
    private int blockBitsX;
    private int blockBitsY;
    private int blockDataOffset;
    /** The end of the packed data; bytes up to the array length beyond it may belong to other values. */
    private final int dataEnd;

    private VertexLookupTable(
        byte[] bytes,
        int directoryOffset,
        int dataOffset,
        int numVertices,
        int baseX,
        int baseY,
        int widthX,
        int widthY,
        int offsetWidth,
        int dataEnd
    ) {
        this.bytes = bytes;
        this.dataEnd = dataEnd;
        this.directoryOffset = directoryOffset;
        this.dataOffset = dataOffset;
        this.numVertices = numVertices;
        this.baseX = baseX;
        this.baseY = baseY;
        this.widthX = widthX;
        this.widthY = widthY;
        this.offsetWidth = offsetWidth;
        this.entryStride = entryStride(widthX, widthY, offsetWidth);
        this.minXShift = fieldShift(0, widthX);
        this.minXMask = fieldMask(widthX);
        this.minYShift = fieldShift(widthX, widthY);
        this.minYMask = fieldMask(widthY);
        this.offsetShift = fieldShift(2, offsetWidth);
        this.offsetMask = fieldMask(offsetWidth);
    }

    /**
     * The right shift that brings a field of {@code width} bytes at byte {@code position} to the low bits of a
     * big-endian long read at byte 0. The two block minimums fit in one long, and so do the bit widths and data offset.
     */
    private static int fieldShift(int position, int width) {
        return Long.SIZE - Byte.SIZE * (position + width);
    }

    private static long fieldMask(int width) {
        return width == 0 ? 0 : (1L << (Byte.SIZE * width)) - 1;
    }

    private static int entryStride(int widthX, int widthY, int offsetWidth) {
        return widthX + widthY + 2 + offsetWidth;
    }

    /** Returns the encoded x-coordinate for the given vertex ordinal. */
    public int getX(int ordinal) {
        loadBlock(ordinal >>> BLOCK_SHIFT);
        int bitPosition = (ordinal & BLOCK_MASK) * (blockBitsX + blockBitsY);
        return blockMinX + readBits(bytes, blockDataOffset, dataEnd, bitPosition, blockBitsX);
    }

    /** Returns the encoded y-coordinate for the given vertex ordinal. */
    public int getY(int ordinal) {
        loadBlock(ordinal >>> BLOCK_SHIFT);
        int bitPosition = (ordinal & BLOCK_MASK) * (blockBitsX + blockBitsY) + blockBitsX;
        return blockMinY + readBits(bytes, blockDataOffset, dataEnd, bitPosition, blockBitsY);
    }

    /**
     * Returns both encoded coordinates of the given vertex ordinal, x in the high 32 bits and y in the low 32 bits.
     * Cheaper than {@link #getX} followed by {@link #getY}, since both are usually extracted from a single read.
     */
    public long getXY(int ordinal) {
        loadBlock(ordinal >>> BLOCK_SHIFT);
        int bitsPerVertex = blockBitsX + blockBitsY;
        int bitPosition = (ordinal & BLOCK_MASK) * bitsPerVertex;
        int start = blockDataOffset + (bitPosition >>> 3);
        int shift = bitPosition & 7;
        if (shift + bitsPerVertex <= Long.SIZE && start + Long.BYTES <= bytes.length) {
            long word = ByteUtils.readLongBE(bytes, start) << shift;
            int x = blockMinX + (int) ((word >>> 1 >>> (Long.SIZE - 1 - blockBitsX)));
            int y = blockMinY + (int) ((word << blockBitsX) >>> 1 >>> (Long.SIZE - 1 - blockBitsY));
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
        int x = blockMinX + readBits(bytes, blockDataOffset, dataEnd, bitPosition, blockBitsX);
        int y = blockMinY + readBits(bytes, blockDataOffset, dataEnd, bitPosition + blockBitsX, blockBitsY);
        return ((long) x << 32) | (y & 0xFFFFFFFFL);
    }

    /**
     * Decodes {@code count} consecutive vertices starting at {@code ordinal} into {@code x} and {@code y} from
     * {@code offset}. Much cheaper per vertex than {@link #getX}/{@link #getY}, since each block is set up only once.
     */
    public void decode(int ordinal, int count, CoordinateEncoder encoder, double[] x, double[] y, int offset) {
        int end = ordinal + count;
        while (ordinal < end) {
            loadBlock(ordinal >>> BLOCK_SHIFT);
            int blockEnd = Math.min(end, (ordinal | BLOCK_MASK) + 1);
            int bitsPerVertex = blockBitsX + blockBitsY;
            int bitPosition = (ordinal & BLOCK_MASK) * bitsPerVertex;
            for (; ordinal < blockEnd; ordinal++, offset++, bitPosition += bitsPerVertex) {
                int start = blockDataOffset + (bitPosition >>> 3);
                int shift = bitPosition & 7;
                if (shift + bitsPerVertex <= Long.SIZE && start + Long.BYTES <= bytes.length) {
                    long word = ByteUtils.readLongBE(bytes, start) << shift;
                    x[offset] = encoder.decodeX(blockMinX + (int) (word >>> 1 >>> (Long.SIZE - 1 - blockBitsX)));
                    y[offset] = encoder.decodeY(blockMinY + (int) ((word << blockBitsX) >>> 1 >>> (Long.SIZE - 1 - blockBitsY)));
                } else {
                    x[offset] = encoder.decodeX(blockMinX + readBits(bytes, blockDataOffset, dataEnd, bitPosition, blockBitsX));
                    y[offset] = encoder.decodeY(
                        blockMinY + readBits(bytes, blockDataOffset, dataEnd, bitPosition + blockBitsX, blockBitsY)
                    );
                }
            }
        }
    }

    private void loadBlock(int block) {
        if (block == cachedBlock) {
            return;
        }
        int entry = directoryOffset + block * entryStride;
        int bitsEntry = entry + widthX + widthY;
        if (bitsEntry + Long.BYTES <= bytes.length) {
            long mins = ByteUtils.readLongBE(bytes, entry);
            blockMinX = baseX + (int) ((mins >>> minXShift) & minXMask);
            blockMinY = baseY + (int) ((mins >>> minYShift) & minYMask);
            long rest = ByteUtils.readLongBE(bytes, bitsEntry);
            blockBitsX = (int) (rest >>> 56);
            blockBitsY = (int) ((rest >>> 48) & 0xFF);
            blockDataOffset = dataOffset + (int) ((rest >>> offsetShift) & offsetMask);
            cachedBlock = block;
            return;
        }
        blockMinX = baseX + readUnsigned(bytes, entry, widthX);
        blockMinY = baseY + readUnsigned(bytes, entry + widthX, widthY);
        blockBitsX = bytes[entry + widthX + widthY] & 0xFF;
        blockBitsY = bytes[entry + widthX + widthY + 1] & 0xFF;
        blockDataOffset = dataOffset + readUnsigned(bytes, entry + widthX + widthY + 2, offsetWidth);
        cachedBlock = block;
    }

    /** Returns the number of vertices in the table. */
    public int size() {
        return numVertices;
    }

    private static int numBlocks(int numVertices) {
        return (numVertices + BLOCK_MASK) >>> BLOCK_SHIFT;
    }

    /**
     * Reads a vertex table from the input stream without allocating coordinate arrays.
     * The returned table reads directly from the underlying byte array on each {@link #getX}/{@link #getY} call.
     *
     * @param input the stream positioned at the start of the vertex table
     * @param bytes the underlying byte array backing the stream (typically {@code BytesRef.bytes})
     * @param extent the extent of the same doc value, which the block minimums are relative to
     */
    public static VertexLookupTable readFrom(ByteArrayStreamInput input, byte[] bytes, Extent extent) throws IOException {
        int size = input.readVInt();
        if (size == 0) {
            return new VertexLookupTable(bytes, input.getPosition(), input.getPosition(), 0, 0, 0, 0, 0, 0, input.getPosition());
        }
        int layout = input.readByte() & 0xFF;
        int numBlocks = numBlocks(size);
        int offsetWidth = numBlocks > 1 ? input.readByte() & 0xFF : 0;
        int widthX = layout & WIDTH_MASK;
        int widthY = (layout >> Y_WIDTH_SHIFT) & WIDTH_MASK;
        int baseX = (layout & ABSOLUTE_X) != 0 ? 0 : extent.minX();
        int baseY = (layout & ABSOLUTE_Y) != 0 ? 0 : extent.minY();
        int directoryOffset = input.getPosition();
        int stride = entryStride(widthX, widthY, offsetWidth);
        // The data length is the last block's offset plus its packed length.
        int lastEntry = directoryOffset + (numBlocks - 1) * stride;
        int lastOffset = readUnsigned(bytes, lastEntry + widthX + widthY + 2, offsetWidth);
        int lastBits = (bytes[lastEntry + widthX + widthY] & 0xFF) + (bytes[lastEntry + widthX + widthY + 1] & 0xFF);
        int lastCount = size - ((numBlocks - 1) << BLOCK_SHIFT);
        int dataOffset = directoryOffset + numBlocks * stride;
        int dataEnd = dataOffset + lastOffset + packedBytes(lastCount, lastBits);
        input.setPosition(dataEnd);
        return new VertexLookupTable(bytes, directoryOffset, dataOffset, size, baseX, baseY, widthX, widthY, offsetWidth, dataEnd);
    }

    private static int packedBytes(int count, int bitsPerVertex) {
        return Math.toIntExact(((long) count * bitsPerVertex + 7) >>> 3);
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
            case 4 -> ByteUtils.readIntBE(bytes, offset);
            default -> throw new IllegalStateException("invalid vertex coordinate width [" + width + "]");
        };
    }

    /**
     * Reads {@code width} (0 to 32) bits starting {@code bitPosition} bits after {@code offset}, most significant bit first.
     * A 32-bit value is returned wrapped, like {@link #readUnsigned}. Reads a whole big-endian long when the array
     * allows it, since at most 39 bits (7 bits of shift plus 32 bits) are ever needed.
     */
    private static int readBits(byte[] bytes, int offset, int end, int bitPosition, int width) {
        int start = offset + (bitPosition >>> 3);
        int shift = bitPosition & 7;
        long mask = (1L << width) - 1;
        if (start + Long.BYTES <= bytes.length) {
            return (int) ((ByteUtils.readLongBE(bytes, start) >>> (Long.SIZE - shift - width)) & mask);
        }
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (start + i < end ? bytes[start + i] & 0xFF : 0);
        }
        return (int) ((value >>> (Long.SIZE - shift - width)) & mask);
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
     * the ordinal of an existing vertex. The block bit widths are only known once all vertices
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
         * Serializes the vertex table, with block minimums relative to the minimum of {@code extent},
         * which must be the extent written to the same doc value.
         */
        public void writeTo(StreamOutput out, Extent extent) throws IOException {
            out.writeVInt(nextOrdinal);
            if (nextOrdinal == 0) {
                return;
            }
            final int numBlocks = numBlocks(nextOrdinal);
            final int[] minXs = new int[numBlocks];
            final int[] minYs = new int[numBlocks];
            final int[] bitsX = new int[numBlocks];
            final int[] bitsY = new int[numBlocks];
            final int[] offsets = new int[numBlocks];
            int dataLength = 0;
            for (int block = 0; block < numBlocks; block++) {
                int start = block << BLOCK_SHIFT;
                int end = Math.min(nextOrdinal, start + BLOCK_SIZE);
                int minX = Integer.MAX_VALUE, minY = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE, maxY = Integer.MIN_VALUE;
                for (int i = start; i < end; i++) {
                    minX = Math.min(minX, xs[i]);
                    minY = Math.min(minY, ys[i]);
                    maxX = Math.max(maxX, xs[i]);
                    maxY = Math.max(maxY, ys[i]);
                }
                minXs[block] = minX;
                minYs[block] = minY;
                bitsX[block] = bitWidth((long) maxX - minX);
                bitsY[block] = bitWidth((long) maxY - minY);
                offsets[block] = dataLength;
                dataLength += packedBytes(end - start, bitsX[block] + bitsY[block]);
            }
            boolean absoluteX = min(minXs) < extent.minX();
            boolean absoluteY = min(minYs) < extent.minY();
            int baseX = absoluteX ? 0 : extent.minX();
            int baseY = absoluteY ? 0 : extent.minY();
            int widthX = absoluteX ? Integer.BYTES : byteWidth(minXs, baseX);
            int widthY = absoluteY ? Integer.BYTES : byteWidth(minYs, baseY);
            int offsetWidth = numBlocks > 1 ? (bitWidth(offsets[numBlocks - 1]) + 7) / 8 : 0;
            out.writeByte((byte) (widthX | (widthY << Y_WIDTH_SHIFT) | (absoluteX ? ABSOLUTE_X : 0) | (absoluteY ? ABSOLUTE_Y : 0)));
            if (numBlocks > 1) {
                out.writeByte((byte) offsetWidth);
            }
            int stride = entryStride(widthX, widthY, offsetWidth);
            byte[] directory = new byte[numBlocks * stride];
            byte[] data = new byte[dataLength];
            for (int block = 0; block < numBlocks; block++) {
                int entry = block * stride;
                writeUnsigned(directory, entry, widthX, minXs[block] - baseX);
                writeUnsigned(directory, entry + widthX, widthY, minYs[block] - baseY);
                directory[entry + widthX + widthY] = (byte) bitsX[block];
                directory[entry + widthX + widthY + 1] = (byte) bitsY[block];
                writeUnsigned(directory, entry + widthX + widthY + 2, offsetWidth, offsets[block]);
                int position = offsets[block];
                int start = block << BLOCK_SHIFT;
                int end = Math.min(nextOrdinal, start + BLOCK_SIZE);
                // Accumulates bits most significant first, flushing whole bytes; holds at most 7 + 32 bits.
                long pending = 0;
                int pendingBits = 0;
                for (int i = start; i < end; i++) {
                    pending = (pending << bitsX[block]) | ((long) xs[i] - minXs[block]);
                    pendingBits += bitsX[block];
                    while (pendingBits >= Byte.SIZE) {
                        pendingBits -= Byte.SIZE;
                        data[position++] = (byte) (pending >>> pendingBits);
                    }
                    pending = (pending << bitsY[block]) | ((long) ys[i] - minYs[block]);
                    pendingBits += bitsY[block];
                    while (pendingBits >= Byte.SIZE) {
                        pendingBits -= Byte.SIZE;
                        data[position++] = (byte) (pending >>> pendingBits);
                    }
                }
                if (pendingBits > 0) {
                    data[position] = (byte) (pending << (Byte.SIZE - pendingBits));
                }
            }
            out.writeBytes(directory, 0, directory.length);
            out.writeBytes(data, 0, data.length);
        }

        private static int min(int[] values) {
            int min = Integer.MAX_VALUE;
            for (int value : values) {
                min = Math.min(min, value);
            }
            return min;
        }

        /** The number of bytes needed for the largest unsigned offset of {@code values} from {@code base}. */
        private static int byteWidth(int[] values, int base) {
            long maxOffset = 0;
            for (int value : values) {
                maxOffset = Math.max(maxOffset, (long) value - base);
            }
            return (bitWidth(maxOffset) + 7) / 8;
        }

        private static int bitWidth(long value) {
            return Long.SIZE - Long.numberOfLeadingZeros(value);
        }

        private static long packCoordinates(int x, int y) {
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
    }
}
