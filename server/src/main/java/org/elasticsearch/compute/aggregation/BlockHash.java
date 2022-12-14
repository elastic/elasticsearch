/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.compute.data.BytesRefArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * A specialized hash table implementation maps values of a {@link Block} to ids (in longs).
 * This class delegates to {@link LongHash} or {@link BytesRefHash}.
 *
 * @see LongHash
 * @see BytesRefHash
 */
public abstract class BlockHash implements Releasable {

    /**
     * Try to add the value (as the key) at the given position of the Block to the hash.
     * Return its newly allocated id if it wasn't in the hash table yet, or {@code -1}
     * if it was already present in the hash table.
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
     * Creates a specialized hash table that maps a {@link Block} of longs to ids.
     */
    public static BlockHash newLongHash(BigArrays bigArrays) {
        return new LongBlockHash(bigArrays);
    }

    /**
     * Creates a specialized hash table that maps a {@link Block} of BytesRefs to ids.
     */
    public static BlockHash newBytesRefHash(BigArrays bigArrays) {
        return new BytesRefBlockHash(bigArrays);
    }

    private static class LongBlockHash extends BlockHash {
        private final LongHash longHash;

        LongBlockHash(BigArrays bigArrays) {
            this.longHash = new LongHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            return longHash.add(block.getLong(position));
        }

        @Override
        public Block getKeys() {
            final int size = Math.toIntExact(longHash.size());
            final long[] keys = new long[size];
            for (int i = 0; i < size; i++) {
                keys[i] = longHash.get(i);
            }

            // TODO call something like takeKeyOwnership to claim the keys array directly
            return new LongArrayBlock(keys, keys.length);
        }

        @Override
        public void close() {
            longHash.close();
        }
    }

    private static class BytesRefBlockHash extends BlockHash {
        private final BytesRefHash bytesRefHash;
        private BytesRef bytes = new BytesRef();

        BytesRefBlockHash(BigArrays bigArrays) {
            this.bytesRefHash = new BytesRefHash(1, bigArrays);
        }

        @Override
        public long add(Block block, int position) {
            bytes = block.getBytesRef(position, bytes);
            return bytesRefHash.add(bytes);
        }

        @Override
        public Block getKeys() {
            final int size = Math.toIntExact(bytesRefHash.size());
            /*
             * Create an un-owned copy of the data so we can close our BytesRefHash
             * without and still read from the block.
             */
            // TODO replace with takeBytesRefsOwnership ?!
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                bytesRefHash.getBytesRefs().writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    return new BytesRefArrayBlock(size, new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE));
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
}
