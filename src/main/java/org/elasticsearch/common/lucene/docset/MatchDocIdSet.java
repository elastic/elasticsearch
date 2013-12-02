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
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 * A {@link DocIdSet} that works on a "doc" level by checking if it matches or not.
 */
public abstract class MatchDocIdSet extends DocIdSet implements Bits {

    private final int maxDoc;
    private final Bits acceptDocs;

    protected MatchDocIdSet(int maxDoc, @Nullable Bits acceptDocs) {
        this.maxDoc = maxDoc;
        this.acceptDocs = acceptDocs;
    }

    /**
     * Does this document match?
     */
    protected abstract boolean matchDoc(int doc);

    @Override
    public DocIdSetIterator iterator() throws IOException {
        if (acceptDocs == null) {
            return new NoAcceptDocsIterator(maxDoc);
        } else if (acceptDocs instanceof FixedBitSet) {
            return new FixedBitSetIterator(((DocIdSet) acceptDocs).iterator());
        } else {
            return new BothIterator(maxDoc, acceptDocs);
        }
    }

    @Override
    public Bits bits() throws IOException {
        return this;
    }

    @Override
    public boolean get(int index) {
        return matchDoc(index);
    }

    @Override
    public int length() {
        return maxDoc;
    }

    class NoAcceptDocsIterator extends DocIdSetIterator {

        private final int maxDoc;
        private int doc = -1;

        NoAcceptDocsIterator(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            do {
                doc++;
                if (doc >= maxDoc) {
                    return doc = NO_MORE_DOCS;
                }
            } while (!matchDoc(doc));
            return doc;
        }

        @Override
        public int advance(int target) {
            for (doc = target; doc < maxDoc; doc++) {
                if (matchDoc(doc)) {
                    return doc;
                }
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return maxDoc;
        }

    }

    class FixedBitSetIterator extends FilteredDocIdSetIterator {

        FixedBitSetIterator(DocIdSetIterator innerIter) {
            super(innerIter);
        }

        @Override
        protected boolean match(int doc) {
            return matchDoc(doc);
        }
    }

    class BothIterator extends DocIdSetIterator {
        private final int maxDoc;
        private final Bits acceptDocs;
        private int doc = -1;

        BothIterator(int maxDoc, Bits acceptDocs) {
            this.maxDoc = maxDoc;
            this.acceptDocs = acceptDocs;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            do {
                doc++;
                if (doc >= maxDoc) {
                    return doc = NO_MORE_DOCS;
                }
            } while (!(matchDoc(doc) && acceptDocs.get(doc)));
            return doc;
        }

        @Override
        public int advance(int target) {
            for (doc = target; doc < maxDoc; doc++) {
                if (matchDoc(doc) && acceptDocs.get(doc)) {
                    return doc;
                }
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }
}
