/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 *  Specialized hash table implementation similar to Lucene's BytesRefHash that maps
 *  BytesRef values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays}, hashes are cached for faster
 *  re-hashing and capacity is always a multiple of 2 for faster identification of buckets.
 *  This class is not thread-safe.
 */
public abstract class AbstractBytesRefHash extends AbstractHash {

    protected LongArray startOffsets;
    protected ByteArray bytes;
    protected IntArray hashes;
    protected final BytesRef spare;

    // Constructor with configurable capacity and load factor.
    public AbstractBytesRefHash(long capacity, float maxLoadFactor, boolean clearOnResize, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        boolean success = false;
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            startOffsets = bigArrays.newLongArray(capacity + 1, clearOnResize);
            startOffsets.set(0, 0);
            bytes = bigArrays.newByteArray(capacity * 3, clearOnResize);
            hashes = bigArrays.newIntArray(capacity, clearOnResize);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
        spare = new BytesRef();
    }

    protected AbstractBytesRefHash(
        long capacity,
        float maxLoadFactor,
        long size,
        long maxSize,
        LongArray ids,
        LongArray startOffsets,
        ByteArray bytes,
        IntArray hashes,
        BigArrays bigArrays
    ) {
        super(capacity, maxLoadFactor, size, maxSize, ids, bigArrays);

        this.startOffsets = startOffsets;
        this.bytes = bytes;
        this.hashes = hashes;

        spare = new BytesRef();
    }

    /**
     * Return the key at <code>0 &lt;= index &lt;= capacity()</code>. The result is undefined if the slot is unused.
     * <p>Beware that the content of the {@link BytesRef} may become invalid as soon as {@link #close()} is called</p>
     */
    public BytesRef get(long id, BytesRef dest) {
        final long startOffset = startOffsets.get(id);
        final int length = (int) (startOffsets.get(id + 1) - startOffset);
        bytes.get(startOffset, length, dest);
        return dest;
    }

    /**
     * Get the id associated with <code>key</code>
     */
    public long find(BytesRef key, int code) {
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1L || key.bytesEquals(get(id, spare))) {
                return id;
            }
        }
    }

    /** Sugar for {@link #find(BytesRef, int) find(key, key.hashCode()} */
    public abstract long find(BytesRef key);

    private long set(BytesRef key, int code, long id) {
        // assert rehash(key.hashCode()) == code;
        assert size < maxSize;
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key, code);
                ++size;
                return id;
            } else if (key.bytesEquals(get(curId, spare))) {
                return -1 - curId;
            }
        }
    }

    private void append(long id, BytesRef key, int code) {
        assert size == id;
        final long startOffset = startOffsets.get(size);
        bytes = bigArrays.grow(bytes, startOffset + key.length);
        bytes.set(startOffset, key.bytes, key.offset, key.length);
        startOffsets = bigArrays.grow(startOffsets, size + 2);
        startOffsets.set(size + 1, startOffset + key.length);
        hashes = bigArrays.grow(hashes, id + 1);
        hashes.set(id, code);
    }

    private void reset(int code, long id) {
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                break;
            }
        }
    }

    /**
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or <code>-1-id</code>
     * if it was already present in the hash table.
     */
    public long add(BytesRef key, int code) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, code, size);
    }

    /** Sugar to {@link #add(BytesRef, int) add(key, key.hashCode()}. */
    public abstract long add(BytesRef key);

    @Override
    protected void removeAndAdd(long index) {
        final long id = id(index, -1);
        assert id >= 0;
        final int code = hashes.get(id);
        reset(code, id);
    }

    @Override
    public void close() {
        try (Releasable releasable = Releasables.wrap(bytes, hashes, startOffsets)) {
            super.close();
        }
    }

}
