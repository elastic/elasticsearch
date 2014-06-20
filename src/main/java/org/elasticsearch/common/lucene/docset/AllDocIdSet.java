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
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * A {@link DocIdSet} that matches all docs up to a {@code maxDoc}.
 */
public class AllDocIdSet extends DocIdSet {

    private final int maxDoc;

    public AllDocIdSet(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    /**
     * Does not go to the reader and ask for data, so can be cached.
     */
    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.NUM_BYTES_INT;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        return new Iterator(maxDoc);
    }

    @Override
    public Bits bits() throws IOException {
        return new Bits.MatchAllBits(maxDoc);
    }

    public static final class Iterator extends DocIdSetIterator {

        private final int maxDoc;
        private int doc = -1;

        public Iterator(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (++doc < maxDoc) {
                return doc;
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            doc = target;
            if (doc < maxDoc) {
                return doc;
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            
            return maxDoc;
        }
    }
}
