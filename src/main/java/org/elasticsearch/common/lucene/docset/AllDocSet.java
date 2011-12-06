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

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.RamUsage;

import java.io.IOException;

/**
 * An always positive
 *
 *
 */
public class AllDocSet extends DocSet {

    private final int maxDoc;

    public AllDocSet(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public int length() {
        return maxDoc;
    }

    @Override
    public boolean get(int doc) {
        return doc < maxDoc;
    }

    @Override
    public long sizeInBytes() {
        return RamUsage.NUM_BYTES_INT;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        return new AllDocIdSetIterator(maxDoc);
    }

    public static final class AllDocIdSetIterator extends DocIdSetIterator {

        private final int maxDoc;

        private int doc = -1;

        public AllDocIdSetIterator(int maxDoc) {
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
    }
}
