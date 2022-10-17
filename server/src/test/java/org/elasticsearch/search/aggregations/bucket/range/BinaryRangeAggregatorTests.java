/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
            if (i == ords.length) {
                return NO_MORE_ORDS;
            }
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

    private static class FakeSortedBinaryDocValues extends SortedBinaryDocValues {

        private final BytesRef[] terms;
        int i;
        long[] ords;

        FakeSortedBinaryDocValues(BytesRef[] terms) {
            this.terms = terms;
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

        FakeSortedBinaryDocValues values = new FakeSortedBinaryDocValues(terms);
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

    public void testSortedBinaryRangeLeafCollectorSingleValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedBinaryRangeLeafCollector(1);
        }
    }

    public void testSortedBinaryRangeLeafCollectorMultiValued() throws Exception {
        final int iters = randomInt(10);
        for (int i = 0; i < iters; ++i) {
            doTestSortedBinaryRangeLeafCollector(5);
        }
    }
}
