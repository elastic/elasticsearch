/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.hash.MurmurHash3;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.*;

/**
 *  Specialized hash table implementation similar to Lucene's BytesRefHash that maps
 *  BytesRef values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays}, hashes are cached for faster
 *  re-hashing and capacity is always a multiple of 2 for faster identification of buckets.
 */
public final class BytesRefHash extends AbstractHash {

    private LongArray startOffsets;
    private ByteArray bytes;
    private IntArray hashes; // we cache hashes for faster re-hashing
    private final BytesRef spare;

    // Constructor with configurable capacity and default maximum load factor.
    public BytesRefHash(long capacity, PageCacheRecycler recycler) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, recycler);
    }

    //Constructor with configurable capacity and load factor.
    public BytesRefHash(long capacity, float maxLoadFactor, PageCacheRecycler recycler) {
        super(capacity, maxLoadFactor, recycler);
        startOffsets = BigArrays.newLongArray(capacity + 1, recycler, false);
        bytes = BigArrays.newByteArray(capacity * 3, recycler, false);
        hashes = BigArrays.newIntArray(capacity, recycler, false);
        spare = new BytesRef();
    }

    // BytesRef has a weak hashCode function so we try to improve it by rehashing using Murmur3
    // Feel free to remove rehashing if BytesRef gets a better hash function
    private static int rehash(int hash) {
        return MurmurHash3.hash(hash);
    }

    /**
     * Return the key at <code>0 &lte; index &lte; capacity()</code>. The result is undefined if the slot is unused.
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
            if (id == -1L || UnsafeUtils.equals(key, get(id, spare))) {
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
            } else if (UnsafeUtils.equals(key, get(curId, spare))) {
                return -1 - curId;
            }
        }
    }

    private void append(long id, BytesRef key, int code) {
        assert size == id;
        final long startOffset = startOffsets.get(size);
        bytes = BigArrays.grow(bytes, startOffset + key.length);
        bytes.set(startOffset, key.bytes, key.offset, key.length);
        startOffsets = BigArrays.grow(startOffsets, size + 2);
        startOffsets.set(size + 1, startOffset + key.length);
        hashes = BigArrays.grow(hashes, id + 1);
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
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or </code>-1-id</code>
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
    protected void removeAndAdd(long index, long id) {
        final int code = hashes.get(id);
        reset(code, id);
    }

    @Override
    public boolean release() {
        boolean success = false;
        try {
            super.release();
            success = true;
        } finally {
            Releasables.release(success, bytes, hashes);
        }
        return true;
    }

}
