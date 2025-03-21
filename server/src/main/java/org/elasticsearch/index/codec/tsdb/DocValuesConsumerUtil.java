/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

class DocValuesConsumerUtil {

    static SortedNumericDocValues mergeSortedNumericValues(List<SortedNumericDocValuesSub> subs, boolean indexIsSorted) throws IOException {
        long cost = 0;
        for (SortedNumericDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<SortedNumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new SortedNumericDocValues() {
            private int docID = -1;
            private SortedNumericDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int docValueCount() {
                return current.values.docValueCount();
            }

        };
    }

    static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

        final SortedNumericDocValues values;
        int docID = -1;

        SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return docID = values.nextDoc();
        }
    }

    static NumericDocValues mergeNumericValues(List<NumericDocValuesSub> subs, boolean indexIsSorted) throws IOException {
        long cost = 0;
        for (NumericDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new NumericDocValues() {
            private int docID = -1;
            private NumericDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public long longValue() throws IOException {
                return current.values.longValue();
            }

        };
    }

    static class NumericDocValuesSub extends DocIDMerger.Sub {

        final NumericDocValues values;
        int docID = -1;

        NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return docID = values.nextDoc();
        }
    }

    static SortedDocValues mergeSortedValues(List<SortedDocValuesSub> subs, boolean indexIsSorted) throws IOException {
        long cost = 0;
        for (SortedDocValuesSub sub : subs) {
            cost += sub.values.cost();
        }
        final long finalCost = cost;

        final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

        return new SortedDocValues() {
            private int docID = -1;
            private SortedDocValuesSub current;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = current.mappedDocID;
                }
                return docID;
            }

            @Override
            public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }

            @Override
            public int ordValue() throws IOException {
                return current.values.ordValue();
            }

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                return current.values.lookupOrd(ord);
            }

            @Override
            public int getValueCount() {
                return current.values.getValueCount();
            }
        };
    }

    static class SortedDocValuesSub extends DocIDMerger.Sub {

        final SortedDocValues values;
        int docID = -1;

        SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values) {
            super(docMap);
            this.values = values;
            assert values.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return docID = values.nextDoc();
        }
    }

}
