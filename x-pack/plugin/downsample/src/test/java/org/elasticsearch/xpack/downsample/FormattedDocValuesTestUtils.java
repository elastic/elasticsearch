/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.fielddata.FormattedDocValues;

import java.io.IOException;
import java.util.Arrays;

final class FormattedDocValuesTestUtils {

    private FormattedDocValuesTestUtils() {}

    static FormattedDocValues withDocIdIterator(IntArrayList docIdsWithValues, Object[] values) {
        if (docIdsWithValues.size() != values.length) {
            throw new IllegalArgumentException("doc ids and values must have the same length");
        }

        final IntObjectHashMap<Object> docIdToValue = IntObjectHashMap.from(docIdsWithValues.toArray(), values);
        final int[] sortedDocIds = docIdsWithValues.toArray();
        final int[] currentDocId = new int[] { -1 };
        final DocIdSetIterator docIdIterator = docIdIterator(sortedDocIds, currentDocId, null);

        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int target) {
                currentDocId[0] = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public Object nextValue() {
                return docIdToValue.get(currentDocId[0]);
            }

            @Override
            public DocIdSetIterator docIdIterator() {
                return docIdIterator;
            }
        };
    }

    static FormattedDocValues withDocIdIterator(IntArrayList sortedDocIdsWithValues, FormattedDocValues delegate) {
        final int[] sortedDocIds = sortedDocIdsWithValues.toArray();
        final int[] currentDocId = new int[] { -1 };
        final DocIdSetIterator docIdIterator = docIdIterator(sortedDocIds, currentDocId, null);

        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int target) throws IOException {
                return delegate.advanceExact(target);
            }

            @Override
            public int docValueCount() throws IOException {
                return delegate.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return delegate.nextValue();
            }

            @Override
            public DocIdSetIterator docIdIterator() {
                return docIdIterator;
            }
        };
    }

    static TrackingFormattedDocValues trackingWithDocIdIterator(IntArrayList docIdsWithValues, Object[] values) {
        if (docIdsWithValues.size() != values.length) {
            throw new IllegalArgumentException("doc ids and values must have the same length");
        }

        final IntObjectHashMap<Object> docIdToValue = IntObjectHashMap.from(docIdsWithValues.toArray(), values);
        final int[] sortedDocIds = docIdsWithValues.toArray();
        Arrays.sort(sortedDocIds);
        final int[] currentDocId = new int[] { -1 };
        final int[] advanceCalls = new int[] { 0 };
        final int[] advanceExactCalls = new int[] { 0 };
        final DocIdSetIterator docIdIterator = docIdIterator(sortedDocIds, currentDocId, advanceCalls);

        return new TrackingFormattedDocValues(docIdIterator, docIdToValue, currentDocId, advanceCalls, advanceExactCalls);
    }

    private static DocIdSetIterator docIdIterator(int[] sortedDocIds, int[] currentDocId, int[] advanceCalls) {
        return new DocIdSetIterator() {
            @Override
            public int advance(int target) {
                if (advanceCalls != null) {
                    advanceCalls[0]++;
                }
                if (currentDocId[0] != -1 && currentDocId[0] != NO_MORE_DOCS && target <= currentDocId[0]) {
                    throw new IllegalArgumentException(
                        "advance target [" + target + "] must be greater than current doc [" + currentDocId[0] + "]"
                    );
                }
                for (int docId : sortedDocIds) {
                    if (docId >= target) {
                        currentDocId[0] = docId;
                        return docId;
                    }
                }
                currentDocId[0] = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                if (currentDocId[0] == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                return advance(currentDocId[0] + 1);
            }

            @Override
            public int docID() {
                return currentDocId[0];
            }

            @Override
            public long cost() {
                return sortedDocIds.length;
            }
        };
    }

    static final class TrackingFormattedDocValues implements FormattedDocValues {
        private final IntObjectHashMap<Object> docIdToValue;
        private final int[] currentDocId;
        private final int[] advanceCalls;
        private final int[] advanceExactCalls;
        private final DocIdSetIterator docIdIterator;

        private TrackingFormattedDocValues(
            DocIdSetIterator docIdIterator,
            IntObjectHashMap<Object> docIdToValue,
            int[] currentDocId,
            int[] advanceCalls,
            int[] advanceExactCalls
        ) {
            this.docIdIterator = docIdIterator;
            this.docIdToValue = docIdToValue;
            this.currentDocId = currentDocId;
            this.advanceCalls = advanceCalls;
            this.advanceExactCalls = advanceExactCalls;
        }

        @Override
        public boolean advanceExact(int target) {
            advanceExactCalls[0]++;
            currentDocId[0] = target;
            return docIdToValue.containsKey(target);
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public Object nextValue() {
            return docIdToValue.get(currentDocId[0]);
        }

        @Override
        public DocIdSetIterator docIdIterator() {
            return docIdIterator;
        }

        int advanceCalls() {
            return advanceCalls[0];
        }

        int advanceExactCalls() {
            return advanceExactCalls[0];
        }
    }

    public enum DocValuesType {
        WITH_ITERATOR,
        WITHOUT_ITERATOR
    }

    static <T> FormattedDocValues createValuesInstance(IntArrayList docIdBuffer, T[] values) {
        return new FormattedDocValues() {

            final IntObjectHashMap<T> docIdToValue = IntObjectHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public T nextValue() {
                return docIdToValue.get(currentDocId);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }
}
