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

import java.io.IOException;

/**
 *
 */
public class NotDocIdSet extends DocIdSet {

    private final DocIdSet set;

    private final int max;

    public NotDocIdSet(DocIdSet set, int max) {
        this.max = max;
        this.set = set;
    }

    @Override
    public boolean isCacheable() {
        // not cacheable, the reason is that by default, when constructing the filter, it is not cacheable,
        // so if someone wants it to be cacheable, we might as well construct a cached version of the result
        return false;
//        return set.isCacheable();
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return new AllDocSet.AllDocIdSetIterator(max);
        }
        return new NotDocIdSetIterator(max, it);
    }

    public static class NotDocIdSetIterator extends DocIdSetIterator {
        private final int max;
        private DocIdSetIterator it1;
        int lastReturn = -1;
        private int innerDocid = -1;

        NotDocIdSetIterator(int max, DocIdSetIterator it) throws IOException {
            this.max = max;
            this.it1 = it;
            if ((innerDocid = it1.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) it1 = null;
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
    }
}
