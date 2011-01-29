/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.common.lucene.versioned;

import org.elasticsearch.common.trove.impl.Constants;
import org.elasticsearch.common.trove.map.hash.TIntIntHashMap;
import org.elasticsearch.common.util.concurrent.ThreadSafe;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An implementation of {@link VersionedMap} based on trove.
 *
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class NativeVersionedMap implements VersionedMap {

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    private final int segmentShift;

    private final Segment[] segments;

    public NativeVersionedMap() {
        this(16);
    }

    public NativeVersionedMap(int concurrencyLevel) {
        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = new Segment[ssize];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment();
        }
    }

    @Override public boolean beforeVersion(int key, int versionToCheck) {
        Segment segment = segmentFor(hash(key));
        segment.rwl.readLock().lock();
        try {
            int result = segment.map.get(key);
            return result == -1 || versionToCheck < result;
        } finally {
            segment.rwl.readLock().unlock();
        }
    }

    @Override public void putVersion(int key, int version) {
        Segment segment = segmentFor(hash(key));
        segment.rwl.writeLock().lock();
        try {
            segment.map.put(key, version);
        } finally {
            segment.rwl.writeLock().unlock();
        }
    }

    @Override public void putVersionIfAbsent(int key, int version) {
        Segment segment = segmentFor(hash(key));
        segment.rwl.writeLock().lock();
        try {
            if (!segment.map.containsKey(key)) {
                segment.map.put(key, version);
            }
        } finally {
            segment.rwl.writeLock().unlock();
        }
    }

    @Override public void clear() {
        for (Segment segment : segments) {
            segment.rwl.writeLock().lock();
            try {
                segment.map.clear();
            } finally {
                segment.rwl.writeLock().unlock();
            }
        }
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash the hash code for the key
     * @return the segment
     */
    final Segment segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    private static class Segment {
        final ReadWriteLock rwl = new ReentrantReadWriteLock();
        final TIntIntHashMap map = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 0, -1);

        private Segment() {

        }
    }
}
