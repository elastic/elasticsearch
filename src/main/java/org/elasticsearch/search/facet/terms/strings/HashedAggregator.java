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
package org.elasticsearch.search.facet.terms.strings;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.util.Arrays;

public class HashedAggregator {
    private int missing;
    private int total;
    private final HashCount hash;
    private final HashCount assertHash = getAssertHash();

    public HashedAggregator() {
        hash = new BytesRefHashHashCount(new BytesRefHash(10, BigArrays.NON_RECYCLING_INSTANCE));
    }

    public void onDoc(int docId, SortedBinaryDocValues values) {
        values.setDocument(docId);
        final int length = values.count();
        int pendingMissing = 1;
        total += length;
        for (int i = 0; i < length; i++) {
            final BytesRef value = values.valueAt(i);
            onValue(docId, value, value.hashCode());
            pendingMissing = 0;
        }
        missing += pendingMissing;
    }

    public void addValue(BytesRef value, int hashCode) {
        final boolean added = hash.addNoCount(value, hashCode);
        assert assertHash.addNoCount(value, hashCode) == added : "asserting counter diverged from current counter - value: "
                + value + " hash: " + hashCode;
    }

    protected void onValue(int docId, BytesRef value, int hashCode) {
        final boolean added = hash.add(value, hashCode);
        // note: we must do a deep copy here the incoming value could have been
        // modified by a script or so
        assert assertHash.add(BytesRef.deepCopyOf(value), hashCode) == added : "asserting counter diverged from current counter - value: "
                + value + " hash: " + hashCode;
    }

    public final int missing() {
        return missing;
    }

    public final int total() {
        return total;
    }

    public final boolean isEmpty() {
        return hash.size() == 0;
    }

    public BytesRefCountIterator getIter() {
        assert hash.size() == assertHash.size();
        return hash.iter();
    }

    public void release() {
        hash.release();
    }

    public static interface BytesRefCountIterator {
        public BytesRef next();

        BytesRef makeSafe();

        public int count();

        public boolean shared();
    }

    public static InternalFacet buildFacet(String facetName, int size, int shardSize, long missing, long total, TermsFacet.ComparatorType comparatorType,
                                           HashedAggregator aggregator) {
        if (aggregator.isEmpty()) {
            return new InternalStringTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalStringTermsFacet.TermEntry>of(), missing, total);
        } else {
            if (shardSize < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(shardSize, comparatorType.comparator());
                BytesRefCountIterator iter = aggregator.getIter();
                while (iter.next() != null) {
                    ordered.insertWithOverflow(new InternalStringTermsFacet.TermEntry(iter.makeSafe(), iter.count()));
                    // maybe we can survive with a 0-copy here if we keep the
                    // bytes ref hash around?
                }
                InternalStringTermsFacet.TermEntry[] list = new InternalStringTermsFacet.TermEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsFacet.TermEntry) ordered.pop());
                }
                return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
            } else {
                BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<>(comparatorType.comparator(), shardSize);
                BytesRefCountIterator iter = aggregator.getIter();
                while (iter.next() != null) {
                    ordered.add(new InternalStringTermsFacet.TermEntry(iter.makeSafe(), iter.count()));
                    // maybe we can survive with a 0-copy here if we keep the
                    // bytes ref hash around?
                }
                return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
            }
        }
    }

    private HashCount getAssertHash() {
        HashCount count = null;
        assert (count = new AssertingHashCount()) != null;
        return count;
    }

    private static interface HashCount {
        public boolean add(BytesRef value, int hashCode);

        public boolean addNoCount(BytesRef value, int hashCode);

        public void release();

        public int size();

        public BytesRefCountIterator iter();
    }

    private static final class BytesRefHashHashCount implements HashCount {
        private final BytesRefHash hash;
        private int[] counts = new int[10];

        public BytesRefHashHashCount(BytesRefHash hash) {
            this.hash = hash;
        }

        @Override
        public boolean add(BytesRef value, int hashCode) {
            int key = (int)hash.add(value, hashCode);
            if (key < 0) {
                key = ((-key) - 1);
            } else if (key >= counts.length) {
                counts = ArrayUtil.grow(counts, key + 1);
            }
            return (counts[key]++) == 0;
        }

        public boolean addNoCount(BytesRef value, int hashCode) {
            int key = (int)hash.add(value, hashCode);
            final boolean added = key >= 0;
            if (key < 0) {
                key = ((-key) - 1);
            } else if (key >= counts.length) {
                counts = ArrayUtil.grow(counts, key + 1);
            }
            return added;
        }

        @Override
        public BytesRefCountIterator iter() {
            return new BytesRefCountIteratorImpl();
        }

        public final class BytesRefCountIteratorImpl implements BytesRefCountIterator {
            final BytesRef spare = new BytesRef();
            private final int size;
            private int current = 0;
            private int currentCount = -1;

            BytesRefCountIteratorImpl() {
                this.size = (int)hash.size();
            }

            public BytesRef next() {
                if (current < size) {
                    currentCount = counts[current];
                    hash.get(current++, spare);
                    return spare;
                }
                currentCount = -1;
                return null;
            }

            @Override
            public BytesRef makeSafe() {
                return BytesRef.deepCopyOf(spare);
            }

            public int count() {
                return currentCount;
            }

            @Override
            public boolean shared() {
                return true;
            }
        }

        @Override
        public int size() {
            return (int)hash.size();
        }

        @Override
        public void release() {
            hash.close();
        }

    }

    private static final class AssertingHashCount implements HashCount { // simple
        // implementation for assertions
        private final ObjectIntOpenHashMap<HashedBytesRef> valuesAndCount = new ObjectIntOpenHashMap<>();
        private HashedBytesRef spare = new HashedBytesRef();

        @Override
        public boolean add(BytesRef value, int hashCode) {
            int adjustedValue = valuesAndCount.addTo(spare.reset(value, hashCode), 1);
            assert adjustedValue >= 1;
            if (adjustedValue == 1) { // only if we added the spare we create a
                // new instance
                spare.bytes = BytesRef.deepCopyOf(value);
                spare = new HashedBytesRef();
                return true;
            }
            return false;
        }

        @Override
        public int size() {
            return valuesAndCount.size();
        }

        @Override
        public BytesRefCountIterator iter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release() {
        }

        @Override
        public boolean addNoCount(BytesRef value, int hashCode) {
            if (!valuesAndCount.containsKey(spare.reset(value, hashCode))) {
                valuesAndCount.addTo(spare.reset(BytesRef.deepCopyOf(value), hashCode), 0);
                spare = new HashedBytesRef(); // reset the reference since we just added to the hash
                return true;
            }
            return false;
        }
    }
}
