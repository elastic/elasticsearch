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
        List<DocIdSet> iterators = new ArrayList<DocIdSet>(sets.length);
        List<Bits> bits = new ArrayList<Bits>(sets.length);
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
            return new IteratorBasedIterator(iterators.toArray(new DocIdSet[iterators.size()]));
        }
        if (iterators.isEmpty()) {
            return new BitsDocIdSetIterator(new AndBits(bits.toArray(new Bits[bits.size()])));
        }
        // combination of both..., first iterating over the "fast" ones, and then checking on the more
        // expensive ones
        return new BitsDocIdSetIterator.FilteredIterator(
                new IteratorBasedIterator(iterators.toArray(new DocIdSet[iterators.size()])),
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
        int lastReturn = -1;
        private DocIdSetIterator[] iterators = null;
        private final long cost;

        IteratorBasedIterator(DocIdSet[] sets) throws IOException {
            iterators = new DocIdSetIterator[sets.length];
            int j = 0;
            long cost = Integer.MAX_VALUE;
            for (DocIdSet set : sets) {
                if (set == null) {
                    lastReturn = DocIdSetIterator.NO_MORE_DOCS; // non matching
                    break;
                } else {
                   
                    DocIdSetIterator dcit = set.iterator();
                    if (dcit == null) {
                        lastReturn = DocIdSetIterator.NO_MORE_DOCS; // non matching
                        break;
                    }
                    iterators[j++] = dcit;
                    cost = Math.min(cost, dcit.cost());
                }
            }
            this.cost = cost;
            if (lastReturn != DocIdSetIterator.NO_MORE_DOCS) {
                lastReturn = (iterators.length > 0 ? -1 : DocIdSetIterator.NO_MORE_DOCS);
            }
        }

        @Override
        public final int docID() {
            return lastReturn;
        }

        @Override
        public final int nextDoc() throws IOException {

            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

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

            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

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
