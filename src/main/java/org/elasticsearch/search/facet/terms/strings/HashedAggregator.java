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
package org.elasticsearch.search.facet.terms.strings;

import java.util.Arrays;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import com.google.common.collect.ImmutableList;

public class HashedAggregator {
    private int missing;
    private int total;
    private final BytesRefHash hash;
    private int[] counts = new int[10];

    public HashedAggregator() {
        this(new BytesRefHash());
    }
    
    public HashedAggregator(BytesRefHash hash) {
        this.hash = hash;
    }
    
    public void onDoc(int docId, BytesValues values) {
        if (values.hasValue(docId)) {
            final Iter iter = values.getIter(docId);
            while(iter.hasNext()) {
                onValue(docId, iter.next(), iter.hash(), values);
                total++;
            }
        } else {
            missing++;
        }
    }
    
    protected BytesRef makesSafe(BytesRef ref, BytesValues values) {
        return values.makeSafe(ref);
    }
    
    protected void onValue(int docId, BytesRef value, int hashCode, BytesValues values) {
        int key = hash.add(value, hashCode);
        if (key < 0) {
            key = ((-key)-1);
        } else if (key >= counts.length) {
            counts = ArrayUtil.grow(counts, key + 1);
        }
        counts[key]++;
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
        return new BytesRefCountIterator();
    }
    
    
    public final class BytesRefCountIterator {
        final BytesRef spare = new BytesRef();
        private final int size;
        private int current = 0;
        private int currentCount = -1;
        BytesRefCountIterator() {
            this.size = hash.size();
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

        public int count() {
            return currentCount;
        }
    }
    
    public static InternalFacet buildFacet(String facetName, int size, long missing, long total, TermsFacet.ComparatorType comparatorType,  HashedAggregator aggregator) {
        if (aggregator.isEmpty()) {
            return new InternalStringTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalStringTermsFacet.TermEntry>of(), missing, total);
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                BytesRefCountIterator iter = aggregator.getIter();
                BytesRef next = null;
                while((next = iter.next()) != null) {
                    ordered.insertWithOverflow(new InternalStringTermsFacet.TermEntry(BytesRef.deepCopyOf(next), iter.count()));
                    // maybe we can survive with a 0-copy here if we keep the bytes ref hash around?
                }
                InternalStringTermsFacet.TermEntry[] list = new InternalStringTermsFacet.TermEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsFacet.TermEntry) ordered.pop());
                }
                return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
            } else {
                BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.TermEntry>(comparatorType.comparator(), size);
                BytesRefCountIterator iter = aggregator.getIter();
                BytesRef next = null;
                while((next = iter.next()) != null) {
                    ordered.add(new InternalStringTermsFacet.TermEntry(BytesRef.deepCopyOf(next), iter.count()));
                    // maybe we can survive with a 0-copy here if we keep the bytes ref hash around?
                }
                return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
            }
        }
    }
}
