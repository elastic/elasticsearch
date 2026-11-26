/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * The columnar form every format gives a collection in, and the one column a scan carries it in.
 * <p>
 * A format describes a batch of parent rows' collection the way columnar formats store one: {@code offsets}, where the
 * elements of parent {@code p} are positions {@code offsets[p]} to {@code offsets[p + 1]}, and one block per leaf of the
 * element, positioned by element. A null position is an absent field; a multivalued one is a leaf that is itself a list.
 * ORC reads the offsets from its list and map vectors; Parquet derives them from repetition levels. Nothing here knows
 * which operator will read the children, or which format produced them.
 * <p>
 * A page's blocks are positioned by parent row, and element-positioned blocks cannot sit beside them, so the scan
 * carries each parent's elements as one value of a {@link BytesRefBlock}: the carrier. {@link #encode} packs, and
 * {@link #decode} unpacks into pages of child rows whose block 0 is each child's parent position, the shape
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} consume whatever the source.
 */
public final class ChildRelationColumns {
    private ChildRelationColumns() {}

    /**
     * Packs the children of {@code parentCount} parents into a carrier block positioned by parent.
     *
     * @param offsets {@code parentCount + 1} element offsets, non-decreasing from 0
     * @param leaves  one block per leaf, each positioned by element, in the carrier's leaf order
     */
    public static BytesRefBlock encode(BlockFactory blockFactory, int parentCount, int[] offsets, Block[] leaves) {
        int elements = offsets[parentCount];
        for (Block leaf : leaves) {
            if (leaf.getPositionCount() != elements) {
                throw new IllegalArgumentException(
                    "collection leaf has [" + leaf.getPositionCount() + "] positions but [" + elements + "] elements"
                );
            }
        }
        BytesRefBuilder scratch = new BytesRefBuilder();
        BytesRef value = new BytesRef();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(parentCount)) {
            for (int p = 0; p < parentCount; p++) {
                scratch.clear();
                int from = offsets[p];
                int to = offsets[p + 1];
                writeVInt(scratch, to - from);
                for (int e = from; e < to; e++) {
                    for (Block leaf : leaves) {
                        writeValues(scratch, leaf, e, value);
                    }
                }
                builder.appendBytesRef(scratch.get());
            }
            return builder.build();
        }
    }

    private static void writeValues(BytesRefBuilder out, Block leaf, int position, BytesRef scratch) {
        int count = leaf.isNull(position) ? 0 : leaf.getValueCount(position);
        writeVInt(out, count);
        int first = leaf.getFirstValueIndex(position);
        for (int i = first; i < first + count; i++) {
            switch (leaf.elementType()) {
                case BOOLEAN -> out.append((byte) (((BooleanBlock) leaf).getBoolean(i) ? 1 : 0));
                case INT -> writeLong(out, ((IntBlock) leaf).getInt(i));
                case LONG -> writeLong(out, ((LongBlock) leaf).getLong(i));
                case DOUBLE -> writeLong(out, Double.doubleToRawLongBits(((DoubleBlock) leaf).getDouble(i)));
                case BYTES_REF -> {
                    BytesRef bytes = ((BytesRefBlock) leaf).getBytesRef(i, scratch);
                    writeVInt(out, bytes.length);
                    out.append(bytes);
                }
                case NULL -> throw new IllegalStateException("a null block has no values");
                default -> throw new IllegalArgumentException("unsupported collection leaf element type [" + leaf.elementType() + "]");
            }
        }
    }

    /**
     * Unpacks the children of the parents at the {@code included} positions of {@code carrier}, in parent order, handing them to
     * {@code consumer} in pages of at most {@code pageSize} children. Block 0 of each page is an {@link IntBlock} vector of
     * each child's parent position; then come the leaves at {@code wanted}, indices into {@code leafTypes}. A parent's remaining
     * children are skipped once {@code keepReading} says its position needs no more; it is asked after each page. Each page is
     * released once {@code consumer} returns. A null carrier position has no children.
     *
     * @param leafTypes the element type of every leaf the carrier holds, in its order
     */
    public static void decode(
        BytesRefBlock carrier,
        ElementType[] leafTypes,
        int[] wanted,
        IntPredicate included,
        IntPredicate keepReading,
        BlockFactory blockFactory,
        int pageSize,
        Consumer<Page> consumer
    ) {
        int[] slotOf = new int[leafTypes.length];
        Arrays.fill(slotOf, -1);
        for (int w = 0; w < wanted.length; w++) {
            slotOf[wanted[w]] = w;
        }
        PageBuilder page = new PageBuilder(blockFactory, leafTypes, wanted, pageSize);
        BytesRef scratch = new BytesRef();
        try {
            for (int p = 0; p < carrier.getPositionCount(); p++) {
                if (included.test(p) == false || carrier.isNull(p)) {
                    continue;
                }
                BytesRef packed = carrier.getBytesRef(carrier.getFirstValueIndex(p), scratch);
                Reader in = new Reader(packed);
                int elements = in.readVInt();
                for (int e = 0; e < elements; e++) {
                    if (keepReading.test(p) == false) {
                        break;
                    }
                    page.startRow(p);
                    for (int l = 0; l < leafTypes.length; l++) {
                        readValues(in, leafTypes[l], slotOf[l] < 0 ? null : page.leaf(slotOf[l]));
                    }
                    if (page.full()) {
                        page.emit(consumer);
                    }
                }
            }
            if (page.rows > 0) {
                page.emit(consumer);
            }
        } finally {
            page.close();
        }
    }

    private static void readValues(Reader in, ElementType type, Block.Builder into) {
        int count = in.readVInt();
        if (count == 0) {
            if (into != null) {
                into.appendNull();
            }
            return;
        }
        if (into != null && count > 1) {
            into.beginPositionEntry();
        }
        for (int i = 0; i < count; i++) {
            switch (type) {
                case BOOLEAN -> {
                    boolean v = in.readByte() != 0;
                    if (into != null) {
                        ((BooleanBlock.Builder) into).appendBoolean(v);
                    }
                }
                case INT -> {
                    long v = in.readLong();
                    if (into != null) {
                        ((IntBlock.Builder) into).appendInt((int) v);
                    }
                }
                case LONG -> {
                    long v = in.readLong();
                    if (into != null) {
                        ((LongBlock.Builder) into).appendLong(v);
                    }
                }
                case DOUBLE -> {
                    long v = in.readLong();
                    if (into != null) {
                        ((DoubleBlock.Builder) into).appendDouble(Double.longBitsToDouble(v));
                    }
                }
                case BYTES_REF -> {
                    int length = in.readVInt();
                    if (into != null) {
                        ((BytesRefBlock.Builder) into).appendBytesRef(new BytesRef(in.bytes, in.offset, length));
                    }
                    in.offset += length;
                }
                default -> throw new IllegalArgumentException("unsupported collection leaf element type [" + type + "]");
            }
        }
        if (into != null && count > 1) {
            into.endPositionEntry();
        }
    }

    /** Builds pages of child rows. */
    private static final class PageBuilder {
        private final BlockFactory blockFactory;
        private final ElementType[] leafTypes;
        private final int[] wanted;
        private final int pageSize;
        private IntBlock.Builder parents;
        private Block.Builder[] leaves;
        int rows;

        PageBuilder(BlockFactory blockFactory, ElementType[] leafTypes, int[] wanted, int pageSize) {
            this.blockFactory = blockFactory;
            this.leafTypes = leafTypes;
            this.wanted = wanted;
            this.pageSize = pageSize;
            reset();
        }

        private void reset() {
            parents = blockFactory.newIntBlockBuilder(pageSize);
            leaves = new Block.Builder[wanted.length];
            for (int w = 0; w < wanted.length; w++) {
                leaves[w] = leafTypes[wanted[w]].newBlockBuilder(pageSize, blockFactory);
            }
            rows = 0;
        }

        void startRow(int parent) {
            parents.appendInt(parent);
            rows++;
        }

        Block.Builder leaf(int slot) {
            return leaves[slot];
        }

        boolean full() {
            return rows >= pageSize;
        }

        void emit(Consumer<Page> consumer) {
            Block[] blocks = new Block[1 + leaves.length];
            Page page;
            try {
                blocks[0] = parents.build();
                for (int w = 0; w < leaves.length; w++) {
                    blocks[w + 1] = leaves[w].build();
                }
                page = new Page(blocks);
                blocks = null;
            } finally {
                if (blocks != null) {
                    Releasables.closeExpectNoException(blocks);
                }
                Releasables.closeExpectNoException(parents);
                Releasables.closeExpectNoException(leaves);
                reset();
            }
            try {
                consumer.accept(page);
            } finally {
                page.releaseBlocks();
            }
        }

        void close() {
            Releasables.closeExpectNoException(parents);
            Releasables.closeExpectNoException(leaves);
        }
    }

    private static void writeVInt(BytesRefBuilder out, int value) {
        while ((value & ~0x7F) != 0) {
            out.append((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.append((byte) value);
    }

    private static void writeLong(BytesRefBuilder out, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.append((byte) (value >>> shift));
        }
    }

    /** Reads what {@link #encode} wrote, with no copying. */
    private static final class Reader {
        final byte[] bytes;
        int offset;

        Reader(BytesRef ref) {
            this.bytes = ref.bytes;
            this.offset = ref.offset;
        }

        byte readByte() {
            return bytes[offset++];
        }

        int readVInt() {
            int value = 0;
            int shift = 0;
            byte b;
            do {
                b = bytes[offset++];
                value |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return value;
        }

        long readLong() {
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bytes[offset++] & 0xFFL);
            }
            return value;
        }
    }
}
