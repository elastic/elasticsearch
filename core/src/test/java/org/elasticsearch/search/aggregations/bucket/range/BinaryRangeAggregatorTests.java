/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.aggregations.bucket.range;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator.SortedBinaryRangeLeafCollector;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator.SortedSetRangeLeafCollector;
import org.elasticsearch.test.ESTestCase;

import com.carrotsearch.hppc.LongHashSet;

public class BinaryRangeAggregatorTests extends ESTestCase {

    private static class FakeSortedSetDocValues extends SortedSetDocValues {

        private final BytesRef[] terms;
        long[] ords;
        private int i;

        FakeSortedSetDocValues(BytesRef[] terms) {
            this.terms = terms;
        }

        @Override
        public void setDocument(int docID) {
            i = 0;
        }

        @Override
        public long nextOrd() {
            if (i == ords.length) {
                return NO_MORE_ORDS;
            }
            return ords[i++];
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
            ranges[i] = new BinaryRangeAggregator.Range(Integer.toString(i),
                    randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))),
                    randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))));
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
            LongHashSet ordinalSet = new LongHashSet();
            final int numValues = randomInt(maxNumValuesPerDoc);
            while (ordinalSet.size() < numValues) {
                ordinalSet.add(random().nextInt(terms.length));
            }
            final long[] ords = ordinalSet.toArray();
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
        long[] ords;

        FakeSortedBinaryDocValues(BytesRef[] terms) {
            this.terms = terms;
        }

        @Override
        public void setDocument(int docID) {
            // no-op
        }

        @Override
        public int count() {
            return ords.length;
        }

        @Override
        public BytesRef valueAt(int index) {
            return terms[(int) ords[index]];
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
            ranges[i] = new BinaryRangeAggregator.Range(Integer.toString(i),
                    randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))),
                    randomBoolean() ? null : new BytesRef(TestUtil.randomSimpleString(random(), randomInt(2))));
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
            LongHashSet ordinalSet = new LongHashSet();
            final int numValues = randomInt(maxNumValuesPerDoc);
            while (ordinalSet.size() < numValues) {
                ordinalSet.add(random().nextInt(terms.length));
            }
            final long[] ords = ordinalSet.toArray();
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
