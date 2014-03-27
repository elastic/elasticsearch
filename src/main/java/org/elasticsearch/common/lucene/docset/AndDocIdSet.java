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
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
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
            return IteratorBasedIterator.newDocIdSetIterator(iterators.toArray(new DocIdSet[iterators.size()]));
        }
        if (iterators.isEmpty()) {
            return new BitsDocIdSetIterator(new AndBits(bits.toArray(new Bits[bits.size()])));
        }
        // combination of both..., first iterating over the "fast" ones, and then checking on the more
        // expensive ones
        return new BitsDocIdSetIterator.FilteredIterator(
                IteratorBasedIterator.newDocIdSetIterator(iterators.toArray(new DocIdSet[iterators.size()])),
                new AndBits(bits.toArray(new Bits[bits.size()]))
        );
    }

    static class AndBits implements Bits {

        private final Bits[] bits;

        AndBits(Bits[] bits) {
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
        private int lastReturn = -1;
        private final DocIdSetIterator[] iterators;
        private final long cost;


        public static DocIdSetIterator newDocIdSetIterator(DocIdSet[] sets) throws IOException {
            if (sets.length == 0) {
                return  DocIdSetIterator.empty();
            }
            final DocIdSetIterator[] iterators = new DocIdSetIterator[sets.length];
            int j = 0;
            long cost = Integer.MAX_VALUE;
            for (DocIdSet set : sets) {
                if (set == null) {
                    return DocIdSetIterator.empty();
                } else {
                    DocIdSetIterator docIdSetIterator = set.iterator();
                    if (docIdSetIterator == null) {
                        return DocIdSetIterator.empty();// non matching
                    }
                    iterators[j++] = docIdSetIterator;
                    cost = Math.min(cost, docIdSetIterator.cost());
                }
            }
            if (sets.length == 1) {
               // shortcut if there is only one valid iterator.
               return iterators[0];
            }
            return new IteratorBasedIterator(iterators, cost);
        }

        private IteratorBasedIterator(DocIdSetIterator[] iterators, long cost) throws IOException {
            this.iterators = iterators;
            this.cost = cost;
        }

        @Override
        public final int docID() {
            return lastReturn;
        }

        @Override
        public final int nextDoc() throws IOException {

            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) {
                assert false : "Illegal State - DocIdSetIterator is already exhausted";
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            DocIdSetIterator dcit = iterators[0];
            int target = dcit.nextDoc();
            int size = iterators.length;
            int skip = 0;
            int i = 1;
            while (i < size) {
                if (i != skip) {
                    dcit = iterators[i];
                    int docid = dcit.advance(target);
                    if (docid > target) {
                        target = docid;
                        if (i != 0) {
                            skip = i;
                            i = 0;
                            continue;
                        } else
                            skip = 0;
                    }
                }
                i++;
            }
            return (lastReturn = target);
        }

        @Override
        public final int advance(int target) throws IOException {

            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) {
                assert false : "Illegal State - DocIdSetIterator is already exhausted";
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            DocIdSetIterator dcit = iterators[0];
            target = dcit.advance(target);
            int size = iterators.length;
            int skip = 0;
            int i = 1;
            while (i < size) {
                if (i != skip) {
                    dcit = iterators[i];
                    int docid = dcit.advance(target);
                    if (docid > target) {
                        target = docid;
                        if (i != 0) {
                            skip = i;
                            i = 0;
                            continue;
                        } else {
                            skip = 0;
                        }
                    }
                }
                i++;
            }
            return (lastReturn = target);
        }

        @Override
        public long cost() {
            return cost;
        }
    }
}
