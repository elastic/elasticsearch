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
 * <h2>Encoding</h2>
 *
 * <p>The coordinates are compressed with block frame-of-reference bit-packing, the scheme Lucene uses for numeric
 * doc values (see {@code Lucene90DocValuesConsumer#writeBlock}) and postings, first described by Goldstein,
 * Ramakrishnan and Shaft in "Compressing Relations and Indexes" (ICDE 1998). Vertices are grouped into blocks of
 * {@link #BLOCK_SIZE} consecutive ordinals, and each block stores every coordinate as its offset from the block
 * minimum, in as many bits as the largest offset in that block needs. Offsets are from the block minimum, not from
 * the previous vertex as in delta coding, so a vertex is decoded without reading the vertices before it. A
 * fixed-size directory entry per block holds the block minimum, the bit widths and the position of the block's
 * packed data, so reading any ordinal is O(1).
 *
 * <p>Since the table is in geometry order, the vertices of a block are close together even when the whole shape
 * is large, which keeps the bit widths small. Compared to Lucene's numeric encoding:
 * <ul>
 *   <li>Each vertex has two coordinates, and each axis has its own block minimum and bit width.</li>
 *   <li>Blocks are small, since a table holds the vertices of a single geometry rather than of a whole segment.</li>
 *   <li>The block minimums are frame-of-reference encoded too: as unsigned offsets from the minimum of the
 *   document's {@link Extent}, in a whole number of bytes per axis (0 to 4) chosen per document from the largest
 *   offset. If any vertex lies below the extent minimum on an axis (possible for geometry vertices that the
 *   tessellation drops), that axis stores absolute 4-byte block minimums instead.</li>
 * </ul>
 *
 * <h2>Layout</h2>
 *
 * <p>Everything after {@code numVertices} is written only if {@code numVertices > 0}. All multi-byte fields are
 * big-endian.
 * <pre>
 * numVertices    VInt
 * layout         byte    bits 0-2: widthX, bits 3-5: widthY, the bytes per block minimum on each axis
 *                        bit 6: the x block minimums are absolute, bit 7: the y block minimums are absolute
 * offsetWidth    byte    bytes per data offset, only present if there is more than one block
 * directory      one entry per block, each widthX + widthY + 2 + offsetWidth bytes:
 *                  minX - baseX   widthX bytes
 *                  minY - baseY   widthY bytes
 *                  bitsX          byte
 *                  bitsY          byte
 *                  dataOffset     offsetWidth bytes, from the start of the packed data
 * packed data    per block, per vertex: x - minX in bitsX bits then y - minY in bitsY bits, most significant
 *                bit first, with the block padded to a whole byte
 * </pre>
 * where {@code baseX} and {@code baseY} are the extent minimum, or 0 for an absolute axis.
 *
 * <p>For example a block holding (1000, 500), (1003, 502) and (1001, 507) has the minimum (1000, 500) and the
 * offsets (0, 0), (3, 2) and (1, 7). The largest x offset 3 needs 2 bits and the largest y offset 7 needs 3 bits,
 * so each vertex takes 5 bits: {@code 00 000 11 010 01 111}, padded to the two bytes {@code 00000110 10011110}.
 *
 * <p>Values made only of points have no triangle tree or connectivity, and store just the points in this table
 * (see {@link Builder#writePointsTo}). A single point stores only {@code numVertices}, as it is the extent minimum.
 *
 * <h2>Reading</h2>
 *
 * <p>The read path decodes directly from the doc-value byte array, without allocating coordinate arrays. Rather
 * than reading the fields above one at a time, it mostly reads a whole big-endian long and extracts fields with
 * shifts and masks: one long holds both block minimums of a directory entry, another the bit widths and data
 * offset, and another the packed x and y of a vertex. Each of these falls back to a slower read near the end of
 * the array, where a whole long would run past it. The directory entry of the last block read is cached, since
 * ordinals are mostly read in runs from the same block.
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
            int x = blockMinX + unpackX(word, blockBitsX);
            int y = blockMinY + unpackY(word, blockBitsX, blockBitsY);
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
                    x[offset] = encoder.decodeX(blockMinX + unpackX(word, blockBitsX));
                    y[offset] = encoder.decodeY(blockMinY + unpackY(word, blockBitsX, blockBitsY));
                } else {
                    x[offset] = encoder.decodeX(blockMinX + readBits(bytes, blockDataOffset, dataEnd, bitPosition, blockBitsX));
                    y[offset] = encoder.decodeY(
                        blockMinY + readBits(bytes, blockDataOffset, dataEnd, bitPosition + blockBitsX, blockBitsY)
                    );
                }
            }
        }
    }

    /**
     * The x offset of a vertex whose packed bits start at the most significant bit of {@code word}. The shift is
     * split as {@code >>> 1 >>> (63 - bitsX)} rather than {@code >>> (64 - bitsX)} because Java only uses the low six
     * bits of a long shift distance, so a single shift by 64 would leave the word unchanged instead of giving the 0
     * that a zero bit width needs.
     */
    private static int unpackX(long word, int bitsX) {
        return (int) (word >>> 1 >>> (Long.SIZE - 1 - bitsX));
    }

    /** The y offset of a vertex whose packed bits start at the most significant bit of {@code word}, as {@link #unpackX}. */
    private static int unpackY(long word, int bitsX, int bitsY) {
        return (int) ((word << bitsX) >>> 1 >>> (Long.SIZE - 1 - bitsY));
    }

    /**
     * Reads the directory entry of {@code block} into the cached block fields, unless it is already cached. The fast
     * path reads two longs, one starting at the block minimums and one at the bit widths, and shifts out the bytes
     * beyond each field, which belong to the rest of the entry or the entries after it.
     */
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

    /**
     * Visits every vertex as a point, for values made only of points, skipping whole blocks whose bounding box the
     * visitor rejects. A block's box is its minimum plus the largest offset its bit widths can hold, capped at the
     * maximum of the value's extent: a superset of the real box, which is safe for the subtree checks
     * {@link TriangleTreeVisitor#pushX}, {@link TriangleTreeVisitor#pushY} and {@link TriangleTreeVisitor#push(int, int)}.
     *
     * @param extentMaxX the maximum x of the value's extent
     * @param extentMaxY the maximum y of the value's extent
     */
    void visitPoints(TriangleTreeVisitor visitor, int extentMaxX, int extentMaxY) {
        final int numBlocks = numBlocks(numVertices);
        for (int block = 0; block < numBlocks; block++) {
            loadBlock(block);
            int maxX = (int) Math.min(extentMaxX, (long) blockMinX + ((1L << blockBitsX) - 1));
            int maxY = (int) Math.min(extentMaxY, (long) blockMinY + ((1L << blockBitsY) - 1));
            if (visitor.pushX(blockMinX) && visitor.pushY(blockMinY) && visitor.push(maxX, maxY)) {
                int end = Math.min(numVertices, (block + 1) << BLOCK_SHIFT);
                for (int ordinal = block << BLOCK_SHIFT; ordinal < end; ordinal++) {
                    long xy = getXY(ordinal);
                    visitor.visitPoint((int) (xy >>> 32), (int) xy);
                    if (visitor.push() == false) {
                        return;
                    }
                }
            } else if (visitor.push() == false) {
                return;
            }
        }
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
        return readVertices(input, bytes, extent, size);
    }

    /**
     * Reads the vertex table of a value made only of points, as written by {@link Builder#writePointsTo}. A single
     * point is not stored at all, since it is the minimum of the extent.
     *
     * @param input the stream positioned at the start of the table
     * @param bytes the underlying byte array backing the stream (typically {@code BytesRef.bytes})
     * @param extent the extent of the same doc value
     */
    public static VertexLookupTable readPointsFrom(ByteArrayStreamInput input, byte[] bytes, Extent extent) throws IOException {
        int size = input.readVInt();
        if (size == 1) {
            // A single block with zero bit widths: every read returns the block minimum, which is the extent minimum.
            return new VertexLookupTable(SINGLE_VERTEX, 0, 2, 1, extent.minX(), extent.minY(), 0, 0, 0, 2);
        }
        return readVertices(input, bytes, extent, size);
    }

    /** A directory entry with zero block minimum offsets and zero bit widths, padded for whole-long reads. */
    private static final byte[] SINGLE_VERTEX = new byte[Long.BYTES];

    private static VertexLookupTable readVertices(ByteArrayStreamInput input, byte[] bytes, Extent extent, int size) throws IOException {
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
        return builder(16);
    }

    /**
     * Creates a new builder sized for about {@code expectedVertices} vertices. Most values are small geometries, so sizing the
     * builder up front avoids allocating far more than they need, and growing repeatedly for large ones.
     */
    public static Builder builder(int expectedVertices) {
        return new Builder(expectedVertices);
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
        private final int expectedVertices;
        /** Created on first use, since tables of points never look vertices up. */
        private LongIntHashMap coordToOrdinal;
        private int nextOrdinal = 0;
        private int[] xs;
        private int[] ys;

        private Builder(int expectedVertices) {
            this.expectedVertices = Math.max(1, expectedVertices);
            this.xs = new int[this.expectedVertices];
            this.ys = new int[this.expectedVertices];
        }

        private LongIntHashMap coordToOrdinal() {
            if (coordToOrdinal == null) {
                coordToOrdinal = new LongIntHashMap(expectedVertices);
            }
            return coordToOrdinal;
        }

        /**
         * Appends a vertex at the next ordinal, even if the same coordinates were already added.
         * The first ordinal for each coordinate is the one returned by later {@link #addVertex} calls.
         */
        public int appendVertex(int x, int y) {
            int ordinal = append(x, y);
            coordToOrdinal().putIfAbsent(packCoordinates(x, y), ordinal);
            return ordinal;
        }

        /**
         * Adds a vertex to the table. If the vertex already exists, returns its existing ordinal.
         * Otherwise assigns a new sequential ordinal and returns it.
         */
        public int addVertex(int x, int y) {
            long key = packCoordinates(x, y);
            LongIntHashMap coordToOrdinal = coordToOrdinal();
            int index = coordToOrdinal.indexOf(key);
            if (coordToOrdinal.indexExists(index)) {
                return coordToOrdinal.indexGet(index);
            }
            int ordinal = append(x, y);
            coordToOrdinal.indexInsert(index, key, ordinal);
            return ordinal;
        }

        /**
         * Appends a vertex at the next ordinal without making it findable by {@link #addVertex}, for tables of
         * points that have no triangle tree to resolve vertices.
         */
        public int appendUnindexed(int x, int y) {
            return append(x, y);
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
            writeVertices(out, extent);
        }

        /**
         * Serializes the table of a value made only of points, which must hold at least one vertex, for
         * {@link #readPointsFrom}. A single point is written as just the count, since it is the minimum of {@code extent}.
         */
        public void writePointsTo(StreamOutput out, Extent extent) throws IOException {
            assert nextOrdinal > 0 : "a points table needs at least one point";
            out.writeVInt(nextOrdinal);
            if (nextOrdinal > 1) {
                writeVertices(out, extent);
            }
        }

        private void writeVertices(StreamOutput out, Extent extent) throws IOException {
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
