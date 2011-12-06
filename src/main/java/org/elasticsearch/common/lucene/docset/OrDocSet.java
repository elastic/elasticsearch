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
public class OrDocSet extends DocSet {

    private final List<DocSet> sets;

    public OrDocSet(List<DocSet> sets) {
        this.sets = sets;
    }

    @Override
    public boolean get(int doc) {
        for (DocSet s : sets) {
            if (s.get(doc)) return true;
        }
        return false;
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
        return new OrDocIdSetIterator();
    }

    public class OrDocIdSetIterator extends DocIdSetIterator {

        private final class Item {
            public final DocIdSetIterator iter;
            public int doc;

            public Item(DocIdSetIterator iter) {
                this.iter = iter;
                this.doc = -1;
            }
        }

        private int _curDoc;
        private final Item[] _heap;
        private int _size;

        OrDocIdSetIterator() throws IOException {
            _curDoc = -1;
            _heap = new Item[sets.size()];
            _size = 0;
            for (DocIdSet set : sets) {
                DocIdSetIterator iterator = set.iterator();
                if (iterator != null) {
                    _heap[_size++] = new Item(iterator);
                }
            }
            if (_size == 0) _curDoc = DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public final int docID() {
            return _curDoc;
        }

        @Override
        public final int nextDoc() throws IOException {
            if (_curDoc == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

            Item top = _heap[0];
            while (true) {
                DocIdSetIterator topIter = top.iter;
                int docid;
                if ((docid = topIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    top.doc = docid;
                    heapAdjust();
                } else {
                    heapRemoveRoot();
                    if (_size == 0) return (_curDoc = DocIdSetIterator.NO_MORE_DOCS);
                }
                top = _heap[0];
                int topDoc = top.doc;
                if (topDoc > _curDoc) {
                    return (_curDoc = topDoc);
                }
            }
        }

        @Override
        public final int advance(int target) throws IOException {
            if (_curDoc == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

            if (target <= _curDoc) target = _curDoc + 1;

            Item top = _heap[0];
            while (true) {
                DocIdSetIterator topIter = top.iter;
                int docid;
                if ((docid = topIter.advance(target)) != DocIdSetIterator.NO_MORE_DOCS) {
                    top.doc = docid;
                    heapAdjust();
                } else {
                    heapRemoveRoot();
                    if (_size == 0) return (_curDoc = DocIdSetIterator.NO_MORE_DOCS);
                }
                top = _heap[0];
                int topDoc = top.doc;
                if (topDoc >= target) {
                    return (_curDoc = topDoc);
                }
            }
        }

// Organize subScorers into a min heap with scorers generating the earlest document on top.
        /*
        private final void heapify() {
            int size = _size;
            for (int i=(size>>1)-1; i>=0; i--)
                heapAdjust(i);
        }
        */
        /* The subtree of subScorers at root is a min heap except possibly for its root element.
        * Bubble the root down as required to make the subtree a heap.
        */

        private final void heapAdjust() {
            final Item[] heap = _heap;
            final Item top = heap[0];
            final int doc = top.doc;
            final int size = _size;
            int i = 0;

            while (true) {
                int lchild = (i << 1) + 1;
                if (lchild >= size) break;

                Item left = heap[lchild];
                int ldoc = left.doc;

                int rchild = lchild + 1;
                if (rchild < size) {
                    Item right = heap[rchild];
                    int rdoc = right.doc;

                    if (rdoc <= ldoc) {
                        if (doc <= rdoc) break;

                        heap[i] = right;
                        i = rchild;
                        continue;
                    }
                }

                if (doc <= ldoc) break;

                heap[i] = left;
                i = lchild;
            }
            heap[i] = top;
        }

        // Remove the root Scorer from subScorers and re-establish it as a heap

        private void heapRemoveRoot() {
            _size--;
            if (_size > 0) {
                Item tmp = _heap[0];
                _heap[0] = _heap[_size];
                _heap[_size] = tmp; // keep the finished iterator at the end for debugging
                heapAdjust();
            }
        }

    }
}
