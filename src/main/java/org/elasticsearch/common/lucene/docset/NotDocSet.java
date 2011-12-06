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

/**
 *
 */
public class NotDocSet extends GetDocSet {

    private final DocSet set;

    public NotDocSet(DocSet set, int max) {
        super(max);
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
    public boolean get(int doc) {
        return !set.get(doc);
    }

    @Override
    public long sizeInBytes() {
        return set.sizeInBytes();
    }

    // This seems like overhead compared to testing with get and iterating over docs
//    @Override public DocIdSetIterator iterator() throws IOException {
//        return new NotDocIdSetIterator();
//    }
//
//    class NotDocIdSetIterator extends DocIdSetIterator {
//        int lastReturn = -1;
//        private DocIdSetIterator it1 = null;
//        private int innerDocid = -1;
//
//        NotDocIdSetIterator() throws IOException {
//            initialize();
//        }
//
//        private void initialize() throws IOException {
//            it1 = set.iterator();
//
//            if ((innerDocid = it1.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) it1 = null;
//        }
//
//        @Override
//        public int docID() {
//            return lastReturn;
//        }
//
//        @Override
//        public int nextDoc() throws IOException {
//            return advance(0);
//        }
//
//        @Override
//        public int advance(int target) throws IOException {
//
//            if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) {
//                return DocIdSetIterator.NO_MORE_DOCS;
//            }
//
//            if (target <= lastReturn) target = lastReturn + 1;
//
//            if (it1 != null && innerDocid < target) {
//                if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
//                    it1 = null;
//                }
//            }
//
//            while (it1 != null && innerDocid == target) {
//                target++;
//                if (target >= max) {
//                    return (lastReturn = DocIdSetIterator.NO_MORE_DOCS);
//                }
//                if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
//                    it1 = null;
//                }
//            }
//
//            // ADDED THIS, bug in code
//            if (target >= max) {
//                return (lastReturn = DocIdSetIterator.NO_MORE_DOCS);
//            }
//
//            return (lastReturn = target);
//        }
//    }
}
