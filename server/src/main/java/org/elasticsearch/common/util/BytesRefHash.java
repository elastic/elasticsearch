/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 *  Specialized hash table implementation similar to Lucene's BytesRefHash that maps
 *  BytesRef values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays}, hashes are cached for faster
 *  re-hashing and capacity is always a multiple of 2 for faster identification of buckets.
 *  This class is not thread-safe.
 */
public final class BytesRefHash extends AbstractHash implements Accountable {

    // base size of the bytes ref hash
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
        // spare BytesRef
        + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    private final BytesRefArray bytesRefs;
    private final BytesRef spare;

    private IntArray hashes; // we cache hashes for faster re-hashing

    // Constructor with configurable capacity and default maximum load factor.
    public BytesRefHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public BytesRefHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);

        boolean success = false;
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            this.hashes = bigArrays.newIntArray(maxSize, false);
            this.bytesRefs = new BytesRefArray(capacity, bigArrays);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
        spare = new BytesRef();
    }

    /**
     * Construct a BytesRefHash given a BytesRefArray with default maximum load factor.
     *
     * Note the comments below regarding leakage protection of the given BytesRefArray.
     */
    public BytesRefHash(BytesRefArray bytesRefArray, BigArrays bigArrays) {
        this(bytesRefArray, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    /**
     * Construct a BytesRefHash given a BytesRefArray.
     *
     * Note: The ownership over BytesRefArray is taken over by the created BytesRefHash.
     *
     * Because BytesRefHash construction potentially throws a circuit breaker exception, the BytesRefArray must be
     * protected against leakage, e.g.
     *
     * boolean success = false;
     * BytesRefArray array = null;
     * try {
     *     array = new BytesRefArray(...);
     *     hash = new BytesRefHash(array, bigArrays);
     *     success = true;
     * } finally {
     *     if (false == success) {
     *         try (Releasable releasable = Releasables.wrap(array)) {
                    close(); // assuming hash is a member and close() closes it
               }
     *     }
     * }
     *
     * Iff the BytesRefHash instance got created successfully, it is managed by BytesRefHash and does not need to be closed.
     */
    public BytesRefHash(BytesRefArray bytesRefs, float maxLoadFactor, BigArrays bigArrays) {
        super(bytesRefs.size() + 1, maxLoadFactor, bigArrays);

        boolean success = false;
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            this.hashes = bigArrays.newIntArray(maxSize, false);
            this.bytesRefs = BytesRefArray.takeOwnershipOf(bytesRefs);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
        spare = new BytesRef();

        // recreate hashes
        for (int i = 0; i < this.bytesRefs.size(); ++i) {
            this.bytesRefs.get(i, spare);
            reset(rehash(spare.hashCode()), i);
        }

        size = this.bytesRefs.size();
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
        return bytesRefs.get(id, dest);
    }

    /**
     * Get the id associated with <code>key</code>
     */
    public long find(BytesRef key, int code) {
        return find(key, code, spare);
    }

    private long find(BytesRef key, int code, BytesRef intermediate) {
        final long slot = slot(rehash(code), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1L || key.bytesEquals(get(id, intermediate))) {
                return id;
            }
        }
    }

    /** Sugar for {@link #find(BytesRef, int) find(key, key.hashCode()} */
    public long find(BytesRef key) {
        return find(key, key.hashCode());
    }

    /**
     * Allows finding a key in the hash in a thread safe manner, by providing an intermediate
     * BytesRef reference to storing intermediate results. As long as each thread provides
     * its own intermediate instance, this method is thread safe.
     */
    private long threadSafeFind(BytesRef key, BytesRef intermediate) {
        return find(key, key.hashCode(), intermediate);
    }

    private long set(BytesRef key, int code, long id) {
        assert rehash(key.hashCode()) == code;
        assert size < maxSize;
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                setId(index, id);
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
        bytesRefs.append(key);
        hashes.set(id, code);
    }

    private boolean assertConsistent(long id, int code) {
        get(id, spare);
        return rehash(spare.hashCode()) == code;
    }

    private void reset(int code, long id) {
        assert assertConsistent(id, code);
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                setId(index, id);
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
            hashes = bigArrays.resize(hashes, maxSize);
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
        final long id = getAndSetId(index, -1);
        assert id >= 0;
        final int code = hashes.get(id);
        reset(code, id);
    }

    @Override
    public void close() {
        try (Releasable releasable = Releasables.wrap(bytesRefs, hashes)) {
            super.close();
        }
    }

    public BytesRefArray getBytesRefs() {
        return bytesRefs;
    }

    public BytesRefArray takeBytesRefsOwnership() {
        try (Releasable releasable = Releasables.wrap(this)) {
            return BytesRefArray.takeOwnershipOf(bytesRefs);
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + bytesRefs.ramBytesUsed() + ids.ramBytesUsed() + hashes.ramBytesUsed() + spare.bytes.length;
    }

    /**
     * Returns a finder class that can be used to find keys in the hash in a thread-safe manner
     */
    public Finder newFinder() {
        return new Finder();
    }

    public class Finder {
        private final BytesRef intermediate = new BytesRef();

        public long find(BytesRef key) {
            return threadSafeFind(key, intermediate);
        }
    }

}
