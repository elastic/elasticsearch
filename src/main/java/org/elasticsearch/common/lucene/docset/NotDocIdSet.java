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

/**
 * A {@link DocIdSet} that matches the "inverse" of the provided doc id set.
 */
public class NotDocIdSet extends DocIdSet {

    private final DocIdSet set;
    private final int maxDoc;

    public NotDocIdSet(DocIdSet set, int maxDoc) {
        this.maxDoc = maxDoc;
        this.set = set;
    }

    @Override
    public boolean isCacheable() {
        return set.isCacheable();
    }

    @Override
    public Bits bits() throws IOException {
        Bits bits = set.bits();
        if (bits == null) {
            return null;
        }
        return new NotBits(bits);
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return new AllDocIdSet.Iterator(maxDoc);
        }
        // TODO: can we optimize for the FixedBitSet case?
        // if we have bits, its much faster to just check on the flipped end potentially
        // really depends on the nature of the Bits, specifically with FixedBitSet, where
        // most of the docs are set?
        Bits bits = set.bits();
        if (bits != null) {
            return new BitsBasedIterator(bits);
        }
        return new IteratorBasedIterator(maxDoc, it);
    }

    public static class NotBits implements Bits {

        private final Bits bits;

        public NotBits(Bits bits) {
            this.bits = bits;
        }

        @Override
        public boolean get(int index) {
            return !bits.get(index);
        }

        @Override
        public int length() {
            return bits.length();
        }
    }

    public static class BitsBasedIterator extends MatchDocIdSetIterator {

        private final Bits bits;

        public BitsBasedIterator(Bits bits) {
            super(bits.length());
            this.bits = bits;
        }

        @Override
        protected boolean matchDoc(int doc) {
            return !bits.get(doc);
        }

        @Override
        public long cost() {
            return bits.length();
        }
    }

    public static class IteratorBasedIterator extends DocIdSetIterator {
        private final int max;
        private DocIdSetIterator it1;
        private int lastReturn = -1;
        private int innerDocid = -1;
        private final long cost;

        IteratorBasedIterator(int max, DocIdSetIterator it) throws IOException {
            this.max = max;
            this.it1 = it;
            this.cost = it1.cost();
            if ((innerDocid = it1.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) {
                it1 = null;
            }
        }

        @Override
        public int docID() {
            return lastReturn;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(0);
        }

        @Override
        public int advance(int target) throws IOException {

            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            if (target <= lastReturn) target = lastReturn + 1;

            if (it1 != null && innerDocid < target) {
                if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
                    it1 = null;
                }
            }

            while (it1 != null && innerDocid == target) {
                target++;
                if (target >= max) {
                    return (lastReturn = DocIdSetIterator.NO_MORE_DOCS);
                }
                if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
                    it1 = null;
                }
            }

            // ADDED THIS, bug in original code
            if (target >= max) {
                return (lastReturn = DocIdSetIterator.NO_MORE_DOCS);
            }

            return (lastReturn = target);
        }

        @Override
        public long cost() {
            return cost;
        }
    }
}
