/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.util.Arrays;

final class SortedNumericDoubleValuesTestUtils {

    private SortedNumericDoubleValuesTestUtils() {}

    static SortedNumericDoubleValues withDocIdIterator(IntArrayList docIdsWithValues, double... values) {
        return create(docIdsWithValues, true, values);
    }

    static SortedNumericDoubleValues withoutDocIdIterator(IntArrayList docIdsWithValues, double... values) {
        return create(docIdsWithValues, false, values);
    }

    static TrackingSortedNumericDoubleValues trackingWithDocIdIterator(IntArrayList docIdsWithValues, double... values) {
        if (docIdsWithValues.size() != values.length) {
            throw new IllegalArgumentException("doc ids and values must have the same length");
        }

        final IntDoubleHashMap docIdToValue = IntDoubleHashMap.from(docIdsWithValues.toArray(), values);
        final int[] sortedDocIds = docIdsWithValues.toArray();
        Arrays.sort(sortedDocIds);
        final int[] currentDocId = new int[] { -1 };
        final int[] advanceCalls = new int[] { 0 };
        final int[] advanceExactCalls = new int[] { 0 };
        final DocIdSetIterator docIdIterator = docIdIterator(sortedDocIds, currentDocId, advanceCalls);

        return new TrackingSortedNumericDoubleValues(docIdIterator, docIdToValue, currentDocId, advanceCalls, advanceExactCalls);
    }

    private static SortedNumericDoubleValues create(IntArrayList docIdsWithValues, boolean hasDocIdIterator, double... values) {
        if (docIdsWithValues.size() != values.length) {
            throw new IllegalArgumentException("doc ids and values must have the same length");
        }

        final IntDoubleHashMap docIdToValue = IntDoubleHashMap.from(docIdsWithValues.toArray(), values);
        final int[] sortedDocIds = docIdsWithValues.toArray();
        Arrays.sort(sortedDocIds);
        final int[] currentDocId = new int[] { -1 };
        final DocIdSetIterator docIdIterator = hasDocIdIterator ? docIdIterator(sortedDocIds, currentDocId, null) : null;

        return new SortedNumericDoubleValues(docIdIterator) {

            @Override
            public boolean advanceExact(int target) {
                currentDocId[0] = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public double nextValue() {
                return docIdToValue.get(currentDocId[0]);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
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

    static final class TrackingSortedNumericDoubleValues extends SortedNumericDoubleValues {
        private final IntDoubleHashMap docIdToValue;
        private final int[] currentDocId;
        private final int[] advanceCalls;
        private final int[] advanceExactCalls;

        private TrackingSortedNumericDoubleValues(
            DocIdSetIterator docIdIterator,
            IntDoubleHashMap docIdToValue,
            int[] currentDocId,
            int[] advanceCalls,
            int[] advanceExactCalls
        ) {
            super(docIdIterator);
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
        public double nextValue() {
            return docIdToValue.get(currentDocId[0]);
        }

        @Override
        public int docValueCount() {
            return 1;
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
}
