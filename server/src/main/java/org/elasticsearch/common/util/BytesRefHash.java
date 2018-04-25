/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

/**
 *  Specialized hash table implementation similar to Lucene's BytesRefHash that maps
 *  BytesRef values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays}, hashes are cached for faster
 *  re-hashing and capacity is always a multiple of 2 for faster identification of buckets.
 *  This class is not thread-safe.
 */
public final class BytesRefHash extends AbstractHash {

    private LongArray startOffsets;
    private ByteArray bytes;
    private IntArray hashes; // we cache hashes for faster re-hashing
    private final BytesRef spare;

    // Constructor with configurable capacity and default maximum load factor.
    public BytesRefHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    //Constructor with configurable capacity and load factor.
    public BytesRefHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        startOffsets = bigArrays.newLongArray(capacity + 1, false);
        startOffsets.set(0, 0);
        bytes = bigArrays.newByteArray(capacity * 3, false);
        hashes = bigArrays.newIntArray(capacity, false);
        spare = new BytesRef();
    }

    // BytesRef has a weak hashCode function so we try to improve it by rehashing using Murmur3
    // Feel free to remove rehashing if BytesRef gets a better hash function
    private static int rehash(int hash) {
        return BitMixer.mix32(hash);
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
        final long slot = slot(rehash(code), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1L || key.bytesEquals(get(id, spare))) {
                return id;
            }
        }
    }

    /** Sugar for {@link #find(BytesRef, int) find(key, key.hashCode()} */
    public long find(BytesRef key) {
        return find(key, key.hashCode());
    }

    private long set(BytesRef key, int code, long id) {
        assert rehash(key.hashCode()) == code;
        assert size < maxSize;
        final long slot = slot(code, mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
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

    private boolean assertConsistent(long id, int code) {
        get(id, spare);
        return rehash(spare.hashCode()) == code;
    }

    private void reset(int code, long id) {
        assert assertConsistent(id, code);
        final long slot = slot(code, mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
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
        return set(key, rehash(code), size);
    }

    /** Sugar to {@link #add(BytesRef, int) add(key, key.hashCode()}. */
    public long add(BytesRef key) {
        return add(key, key.hashCode());
    }

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
