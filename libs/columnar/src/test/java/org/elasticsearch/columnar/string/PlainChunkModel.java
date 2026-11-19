/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkBounds;

import java.util.List;

/**
 * Which chunk owns each slot of a plain column, worked out from the rules of the chunk writer alone: a chunk is
 * flushed when it holds the byte cap, or at the start of a block of values that would push it past the value cap
 * while it holds bytes. A slot is owned by the chunk being filled once any flush at its start has happened.
 */
final class PlainChunkModel {

    private final ChunkBounds bounds;
    private final int valuesPerBlock;
    private final int lengthBlockSize;
    private final int numValues;
    private final boolean repeatsAllowed;
    private int count;
    private int chunk;
    private int pendingBytes;
    private int pendingValues;

    PlainChunkModel(ChunkBounds bounds, int valuesPerBlock, int lengthBlockSize, int numValues, boolean repeatsAllowed) {
        this.bounds = bounds;
        this.valuesPerBlock = valuesPerBlock;
        this.lengthBlockSize = lengthBlockSize;
        this.numValues = numValues;
        this.repeatsAllowed = repeatsAllowed;
    }

    /** The owner of every slot of a column written the way {@code StringColumnWriter} writes it. */
    static int[] owners(List<BytesRef> slots, ChunkBounds bounds, int valuesPerBlock, int lengthBlockSize) {
        boolean constantLength = true;
        int length = -1;
        for (BytesRef slot : slots) {
            if (slot == null || (length >= 0 && length != slot.length)) {
                constantLength = false;
                break;
            }
            length = slot.length;
        }
        final PlainChunkModel model = new PlainChunkModel(bounds, valuesPerBlock, lengthBlockSize, slots.size(), constantLength == false);
        final int[] owners = new int[slots.size()];
        for (int slot = 0; slot < owners.length; slot++) {
            owners[slot] = model.next(slots.get(slot), slot > 0 ? slots.get(slot - 1) : null);
        }
        return owners;
    }

    int next(BytesRef value, BytesRef previous) {
        if (count % valuesPerBlock == 0) {
            final int values = Math.min(valuesPerBlock, numValues - count);
            if (pendingBytes > 0 && pendingValues + values > bounds.maxValues()) {
                chunk++;
                pendingBytes = 0;
                pendingValues = 0;
            }
            pendingValues += values;
        }
        final int owner = chunk;
        final boolean repeat = repeatsAllowed
            && value != null
            && previous != null
            && previous.bytesEquals(value)
            && count % lengthBlockSize != 0;
        if (value != null && repeat == false) {
            int remaining = value.length;
            while (remaining > 0) {
                final int take = Math.min(bounds.targetBytes() - pendingBytes, remaining);
                pendingBytes += take;
                remaining -= take;
                if (pendingBytes == bounds.targetBytes()) {
                    chunk++;
                    pendingBytes = 0;
                    pendingValues = 0;
                }
            }
        }
        count++;
        return owner;
    }

    /** The number of chunks written once every slot has been taken. */
    int emittedChunks() {
        return chunk + (pendingBytes > 0 ? 1 : 0);
    }
}
