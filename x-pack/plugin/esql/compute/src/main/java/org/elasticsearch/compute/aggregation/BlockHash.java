/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * A specialized hash table implementation maps values of a {@link Block} to ids (in longs).
 * This class delegates to {@link LongHash} or {@link BytesRefHash}.
 *
 * @see LongHash
 * @see BytesRefHash
 */
public abstract sealed class BlockHash implements Releasable {

    /**
     * Try to add the value (as the key) at the given position of the Block to the hash.
     * Return its newly allocated {@code id} if it wasn't in the hash table yet, or
     * {@code -1-id} if it was already present in the hash table.
     *
     * @see LongHash#add(long)
     * @see BytesRefHash#add(BytesRef)
     */
    public abstract long add(Block block, int position);

    /**
     * Returns a {@link Block} that contains all the keys that are inserted by {@link #add(Block, int)}.
     */
    public abstract Block getKeys();

    /**
     * Creates a specialized hash table that maps a {@link Block} of the given input element type to ids.
     */
    public static BlockHash newForElementType(ElementType type, BigArrays bigArrays) {
        return switch (type) {
            case BOOLEAN -> new BooleanBlockHash();
            case INT -> new IntBlockHash(bigArrays);
            case LONG -> new LongBlockHash(bigArrays);
            case DOUBLE -> new DoubleBlockHash(bigArrays);
            case BYTES_REF -> new BytesRefBlockHash(bigArrays);
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + type + "]");
        };
    }

    private static final class LongBlockHash extends BlockHash {
        private final LongHash longHash;

        LongBlockHash(BigArrays bigArrays) {
            this.longHash = new LongHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            return longHash.add(((LongBlock) block).getLong(position));
        }

        @Override
        public LongBlock getKeys() {
            final int size = Math.toIntExact(longHash.size());
            final long[] keys = new long[size];
            for (int i = 0; i < size; i++) {
                keys[i] = longHash.get(i);
            }

            // TODO call something like takeKeyOwnership to claim the keys array directly
            return new LongArrayVector(keys, keys.length).asBlock();
        }

        @Override
        public void close() {
            longHash.close();
        }
    }

    private static final class IntBlockHash extends BlockHash {
        private final LongHash longHash;

        IntBlockHash(BigArrays bigArrays) {
            this.longHash = new LongHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            return longHash.add(((IntBlock) block).getInt(position));
        }

        @Override
        public IntBlock getKeys() {
            final int size = Math.toIntExact(longHash.size());
            final int[] keys = new int[size];
            for (int i = 0; i < size; i++) {
                keys[i] = (int) longHash.get(i);
            }
            return new IntArrayVector(keys, keys.length, null).asBlock();
        }

        @Override
        public void close() {
            longHash.close();
        }
    }

    private static final class DoubleBlockHash extends BlockHash {
        private final LongHash longHash;

        DoubleBlockHash(BigArrays bigArrays) {
            this.longHash = new LongHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            return longHash.add(Double.doubleToLongBits(((DoubleBlock) block).getDouble(position)));
        }

        @Override
        public DoubleBlock getKeys() {
            final int size = Math.toIntExact(longHash.size());
            final double[] keys = new double[size];
            for (int i = 0; i < size; i++) {
                keys[i] = Double.longBitsToDouble(longHash.get(i));
            }
            return new DoubleArrayVector(keys, keys.length).asBlock();
        }

        @Override
        public void close() {
            longHash.close();
        }
    }

    private static final class BytesRefBlockHash extends BlockHash {
        private final BytesRefHash bytesRefHash;
        private BytesRef bytes = new BytesRef();

        BytesRefBlockHash(BigArrays bigArrays) {
            this.bytesRefHash = new BytesRefHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            bytes = ((BytesRefBlock) block).getBytesRef(position, bytes);
            return bytesRefHash.add(bytes);
        }

        @Override
        public BytesRefBlock getKeys() {
            final int size = Math.toIntExact(bytesRefHash.size());
            /*
             * Create an un-owned copy of the data so we can close our BytesRefHash
             * without and still read from the block.
             */
            // TODO replace with takeBytesRefsOwnership ?!
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                bytesRefHash.getBytesRefs().writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    return new BytesRefArrayVector(new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE), size).asBlock();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() {
            bytesRefHash.close();
        }
    }

    /**
     * Assigns group {@code 0} to the first of {@code true} or{@code false}
     * that it sees and {@code 1} to the second.
     */
    private static final class BooleanBlockHash extends BlockHash {
        // TODO this isn't really a "hash" so maybe we should rename base class
        private final int[] buckets = { -1, -1 };

        @Override
        public long add(Block block, int position) {
            boolean b = ((BooleanBlock) block).getBoolean(position);
            int pos = b ? 1 : 0;
            int ord = buckets[pos];
            if (ord == -1) {
                ord = buckets[pos == 0 ? 1 : 0] + 1;
                buckets[pos] = ord;
                return ord;
            } else {
                return -ord - 1;
            }
        }

        @Override
        public BooleanBlock getKeys() {
            BooleanVector.Builder builder = BooleanVector.newVectorBuilder(2);
            if (buckets[0] < buckets[1]) {
                if (buckets[0] >= 0) {
                    builder.appendBoolean(false);
                }
                if (buckets[1] >= 0) {
                    builder.appendBoolean(true);
                }
            } else {
                if (buckets[1] >= 0) {
                    builder.appendBoolean(true);
                }
                if (buckets[0] >= 0) {
                    builder.appendBoolean(false);
                }
            }
            return builder.build().asBlock();
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }
}
