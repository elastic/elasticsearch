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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class AndDocIdSet extends DocIdSet {

    private final DocIdSet[] sets;

    public AndDocIdSet(DocIdSet[] sets) {
        this.sets = sets;
    }

    @Override
    public boolean isCacheable() {
        for (DocIdSet set : sets) {
            if (!set.isCacheable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long ramBytesUsed() {
        long ramBytesUsed = RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        for (DocIdSet set : sets) {
            ramBytesUsed += RamUsageEstimator.NUM_BYTES_OBJECT_REF + set.ramBytesUsed();
        }
        return ramBytesUsed;
    }

    @Override
    public Bits bits() throws IOException {
        Bits[] bits = new Bits[sets.length];
        for (int i = 0; i < sets.length; i++) {
            bits[i] = sets[i].bits();
            if (bits[i] == null) {
                return null;
            }
        }
        return new AndBits(bits);
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        // we try and be smart here, if we can iterate through docsets quickly, prefer to iterate
        // over them as much as possible, before actually going to "bits" based ones to check
        List<DocIdSet> iterators = new ArrayList<>(sets.length);
        List<Bits> bits = new ArrayList<>(sets.length);
        for (DocIdSet set : sets) {
            if (DocIdSets.isFastIterator(set)) {
                iterators.add(set);
            } else {
                Bits bit = set.bits();
                if (bit != null) {
                    bits.add(bit);
                } else {
                    iterators.add(set);
                }
            }
        }
        if (bits.isEmpty()) {
            return IteratorBasedIterator.newDocIdSetIterator(iterators);
        }
        if (iterators.isEmpty()) {
            return new BitsDocIdSetIterator(new AndBits(bits.toArray(new Bits[bits.size()])));
        }
        // combination of both..., first iterating over the "fast" ones, and then checking on the more
        // expensive ones
        return new BitsDocIdSetIterator.FilteredIterator(
                IteratorBasedIterator.newDocIdSetIterator(iterators),
                new AndBits(bits.toArray(new Bits[bits.size()]))
        );
    }

    /** A conjunction between several {@link Bits} instances with short-circuit logic. */
    public static class AndBits implements Bits {

        private final Bits[] bits;

        public AndBits(Bits[] bits) {
            this.bits = bits;
        }

        @Override
        public boolean get(int index) {
            for (Bits bit : bits) {
                if (!bit.get(index)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int length() {
            return bits[0].length();
        }
    }

    static class IteratorBasedIterator extends DocIdSetIterator {
        private int doc = -1;
        private final DocIdSetIterator lead;
        private final DocIdSetIterator[] otherIterators;


        public static DocIdSetIterator newDocIdSetIterator(Collection<DocIdSet> sets) throws IOException {
            if (sets.isEmpty()) {
                return  DocIdSetIterator.empty();
            }
            final DocIdSetIterator[] iterators = new DocIdSetIterator[sets.size()];
            int j = 0;
            for (DocIdSet set : sets) {
                if (set == null) {
                    return DocIdSetIterator.empty();
                } else {
                    DocIdSetIterator docIdSetIterator = set.iterator();
                    if (docIdSetIterator == null) {
                        return DocIdSetIterator.empty();// non matching
                    }
                    iterators[j++] = docIdSetIterator;
                }
            }
            if (sets.size() == 1) {
               // shortcut if there is only one valid iterator.
               return iterators[0];
            }
            return new IteratorBasedIterator(iterators);
        }

        private IteratorBasedIterator(DocIdSetIterator[] iterators) throws IOException {
            final DocIdSetIterator[] sortedIterators = Arrays.copyOf(iterators, iterators.length);
            new InPlaceMergeSorter() {

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(sortedIterators[i].cost(), sortedIterators[j].cost());
                }

                @Override
                protected void swap(int i, int j) {
                    ArrayUtil.swap(sortedIterators, i, j);
                }

            }.sort(0, sortedIterators.length);
            lead = sortedIterators[0];
            this.otherIterators = Arrays.copyOfRange(sortedIterators, 1, sortedIterators.length);
        }

        @Override
        public final int docID() {
            return doc;
        }

        @Override
        public final int nextDoc() throws IOException {
            doc = lead.nextDoc();
            return doNext();
        }

        @Override
        public final int advance(int target) throws IOException {
            doc = lead.advance(target);
            return doNext();
        }

        private int doNext() throws IOException {
            main:
            while (true) {
                for (DocIdSetIterator otherIterator : otherIterators) {
                    // the following assert is the invariant of the loop
                    assert otherIterator.docID() <= doc;
                    // the current doc might already be equal to doc if it broke the loop
                    // at the previous iteration
                    if (otherIterator.docID() < doc) {
                        final int advanced = otherIterator.advance(doc);
                        if (advanced > doc) {
                            doc = lead.advance(advanced);
                            continue main;
                        }
                    }
                }
                return doc;
            }
        }

        @Override
        public long cost() {
            return lead.cost();
        }
    }
}
