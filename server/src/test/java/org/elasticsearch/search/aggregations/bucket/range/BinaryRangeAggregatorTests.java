/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues.ValueOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator.SortedBinaryRangeLeafCollector;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator.SortedSetRangeLeafCollector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class BinaryRangeAggregatorTests extends ESTestCase {

    private static class FakeSortedSetDocValues extends AbstractSortedSetDocValues {

        private final BytesRef[] terms;
        long[] ords;
        private int i;

        FakeSortedSetDocValues(BytesRef[] terms) {
            this.terms = terms;
        }

        @Override
        public boolean advanceExact(int docID) {
            i = 0;
            return true;
        }

        @Override
        public long nextOrd() {
            assert i < ords.length;
            return ords[i++];
        }

        @Override
        public int docValueCount() {
            return ords.length;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            return terms[(int) ord];
        }

        @Override
        public long getValueCount() {
            return terms.length;
        }

    }

    private void doTestSortedSetRangeLeafCollector(int maxNumValuesPerDoc) throws Exception {
        final Set<BytesRef> termSet = new HashSet<>();
        final int numTerms = TestUtil.nextInt(random(), maxNumValuesPerDoc, 100);
        while (termSet.size() < numTerms) {
            termSet.add(new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))));
        }
        final BytesRef[] terms = termSet.toArray(new BytesRef[0]);
        Arrays.sort(terms);

        final int numRanges = randomIntBetween(1, 10);
        BinaryRangeAggregator.Range[] ranges = new BinaryRangeAggregator.Range[numRanges];
        for (int i = 0; i < numRanges; ++i) {
            ranges[i] = new BinaryRangeAggregator.Range(
                Integer.toString(i),
                randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))),
                randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2)))
            );
        }
        Arrays.sort(ranges, BinaryRangeAggregator.RANGE_COMPARATOR);

        FakeSortedSetDocValues values = new FakeSortedSetDocValues(terms);
        final int[] counts = new int[ranges.length];
        SortedSetRangeLeafCollector collector = new SortedSetRangeLeafCollector(values, ranges, null) {
            @Override
            protected void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException {
                counts[(int) bucket]++;
            }
        };

        final int[] expectedCounts = new int[ranges.length];
        final int maxDoc = randomIntBetween(5, 10);
        for (int doc = 0; doc < maxDoc; ++doc) {
            Set<Long> ordinalSet = new HashSet<>();
            final int numValues = randomInt(maxNumValuesPerDoc);
            while (ordinalSet.size() < numValues) {
                ordinalSet.add(random().nextLong(terms.length));
            }
            final long[] ords = ordinalSet.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(ords);
            values.ords = ords;

            // simulate aggregation
            collector.collect(doc);

            // now do it the naive way
            for (int i = 0; i < ranges.length; ++i) {
                for (long ord : ords) {
                    BytesRef term = terms[(int) ord];
                    if ((ranges[i].from == null || ranges[i].from.compareTo(term) <= 0)
                        && (ranges[i].to == null || ranges[i].to.compareTo(term) > 0)) {
                        expectedCounts[i]++;
                        break;
                    }
                }
            }
        }
        assertArrayEquals(expectedCounts, counts);
    }

    public void testSortedSetRangeLeafCollectorSingleValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedSetRangeLeafCollector(1);
        }
    }

    public void testSortedSetRangeLeafCollectorMultiValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedSetRangeLeafCollector(5);
        }
    }

    private static class FakeSortedBinaryDocValues extends SortableBinaryDocValues {

        private final BytesRef[] terms;
        private final ValueOrder valueOrder;
        int i;
        long[] ords;

        FakeSortedBinaryDocValues(BytesRef[] terms) {
            this(terms, ValueOrder.SORTED);
        }

        FakeSortedBinaryDocValues(BytesRef[] terms, ValueOrder valueOrder) {
            super(null);
            this.terms = terms;
            this.valueOrder = valueOrder;
        }

        @Override
        public ValueOrder getValueOrder() {
            return valueOrder;
        }

        @Override
        public boolean advanceExact(int docID) {
            i = 0;
            return true;
        }

        @Override
        public int docValueCount() {
            return ords.length;
        }

        @Override
        public BytesRef nextValue() {
            return terms[(int) ords[i++]];
        }

    }

    private void doTestSortedBinaryRangeLeafCollector(int maxNumValuesPerDoc) throws Exception {
        doTestSortedBinaryRangeLeafCollector(maxNumValuesPerDoc, SortableBinaryDocValues.ValueOrder.SORTED);
    }

    private void doTestSortedBinaryRangeLeafCollector(int maxNumValuesPerDoc, SortableBinaryDocValues.ValueOrder valueOrder)
        throws Exception {
        final Set<BytesRef> termSet = new HashSet<>();
        final int numTerms = TestUtil.nextInt(random(), maxNumValuesPerDoc, 100);
        while (termSet.size() < numTerms) {
            termSet.add(new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))));
        }
        final BytesRef[] terms = termSet.toArray(new BytesRef[0]);
        Arrays.sort(terms);

        final int numRanges = randomIntBetween(1, 10);
        BinaryRangeAggregator.Range[] ranges = new BinaryRangeAggregator.Range[numRanges];
        for (int i = 0; i < numRanges; ++i) {
            ranges[i] = new BinaryRangeAggregator.Range(
                Integer.toString(i),
                randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))),
                randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2)))
            );
        }
        Arrays.sort(ranges, BinaryRangeAggregator.RANGE_COMPARATOR);

        FakeSortedBinaryDocValues values = new FakeSortedBinaryDocValues(terms, valueOrder);
        final int[] counts = new int[ranges.length];
        SortedBinaryRangeLeafCollector collector = new SortedBinaryRangeLeafCollector(values, ranges, null) {
            @Override
            protected void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException {
                counts[(int) bucket]++;
            }
        };

        final int[] expectedCounts = new int[ranges.length];
        final int maxDoc = randomIntBetween(5, 10);
        for (int doc = 0; doc < maxDoc; ++doc) {
            Set<Long> ordinalSet = new HashSet<>();
            final int numValues = randomInt(maxNumValuesPerDoc);
            while (ordinalSet.size() < numValues) {
                ordinalSet.add(random().nextLong(terms.length));
            }
            long[] ords = ordinalSet.stream().mapToLong(Long::longValue).toArray();
            if (ords.length > 0 && randomBoolean()) {
                // The payload keeps repeats, so a document can offer the same value twice. It still belongs to a
                // range once: this counts documents, and nothing deduplicated the field.
                ords = Arrays.copyOf(ords, ords.length + 1);
                ords[ords.length - 1] = ords[randomInt(ords.length - 2)];
            }
            if (valueOrder == SortableBinaryDocValues.ValueOrder.SORTED) {
                Arrays.sort(ords);
            } else {
                // Array order: the collector cannot carry a low bound from one value to the next.
                shuffle(ords);
            }
            values.ords = ords;

            // simulate aggregation
            collector.collect(doc);

            // now do it the naive way
            for (int i = 0; i < ranges.length; ++i) {
                for (long ord : ords) {
                    BytesRef term = terms[(int) ord];
                    if ((ranges[i].from == null || ranges[i].from.compareTo(term) <= 0)
                        && (ranges[i].to == null || ranges[i].to.compareTo(term) > 0)) {
                        expectedCounts[i]++;
                        break;
                    }
                }
            }
        }
        assertArrayEquals(expectedCounts, counts);
    }

    public void testSortedBinaryRangeLeafCollectorSingleValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedBinaryRangeLeafCollector(1);
        }
    }

    /** The same ranges over values that do not ascend, which is how the ColumNAR payload hands them back. */
    public void testSortedBinaryRangeLeafCollectorArrayOrder() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedBinaryRangeLeafCollector(5, SortableBinaryDocValues.ValueOrder.ARRAY);
        }
    }

    private static void shuffle(long[] ords) {
        for (int i = ords.length - 1; i > 0; i--) {
            final int j = randomInt(i);
            final long swap = ords[i];
            ords[i] = ords[j];
            ords[j] = swap;
        }
    }

    public void testSortedBinaryRangeLeafCollectorMultiValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedBinaryRangeLeafCollector(5);
        }
    }
}
