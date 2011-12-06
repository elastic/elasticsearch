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
import java.util.List;

/**
 *
 */
public class AndDocSet extends DocSet {

    private final List<DocSet> sets;

    public AndDocSet(List<DocSet> sets) {
        this.sets = sets;
    }

    @Override
    public boolean get(int doc) {
        for (DocSet s : sets) {
            if (!s.get(doc)) return false;
        }
        return true;
    }

    @Override
    public int length() {
        if (sets.isEmpty()) {
            return 0;
        }
        return sets.get(0).length();
    }

    @Override
    public boolean isCacheable() {
        // not cacheable, the reason is that by default, when constructing the filter, it is not cacheable,
        // so if someone wants it to be cacheable, we might as well construct a cached version of the result
        return false;
//        for (DocSet set : sets) {
//            if (!set.isCacheable()) {
//                return false;
//            }
//        }
//        return true;
    }

    @Override
    public long sizeInBytes() {
        long sizeInBytes = 0;
        for (DocSet set : sets) {
            sizeInBytes += set.sizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        return new AndDocIdSetIterator();
    }

    class AndDocIdSetIterator extends DocIdSetIterator {
        int lastReturn = -1;
        private DocIdSetIterator[] iterators = null;

        AndDocIdSetIterator() throws IOException {
            iterators = new DocIdSetIterator[sets.size()];
            int j = 0;
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
                }
            }
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
    }
}
