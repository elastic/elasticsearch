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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class StatsAggregatorTests extends AggregatorTestCase {
    static final double TOLERANCE = 1e-10;

    public void testEmpty() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        testCase(ft, iw -> {},
            stats -> {
                assertEquals(0d, stats.getCount(), 0);
                assertEquals(0d, stats.getSum(), 0);
                assertEquals(Float.NaN, stats.getAvg(), 0);
                assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0);
                assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testRandomDoubles() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        ft.setName("field");
        final SimpleStatsAggregator expected = new SimpleStatsAggregator();
        testCase(ft,
            iw -> {
                int numDocs = randomIntBetween(10, 50);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    int numValues = randomIntBetween(1, 5);
                    for (int j = 0; j < numValues; j++) {
                        double value = randomDoubleBetween(-100d, 100d, true);
                        long valueAsLong = NumericUtils.doubleToSortableLong(value);
                        doc.add(new SortedNumericDocValuesField("field", valueAsLong));
                        expected.add(value);
                    }
                    iw.addDocument(doc);
                }
            },
            stats -> {
                assertEquals(expected.count, stats.getCount(), 0);
                assertEquals(expected.sum, stats.getSum(), TOLERANCE);
                assertEquals(expected.min, stats.getMin(), 0);
                assertEquals(expected.max, stats.getMax(), 0);
                assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testRandomLongs() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        final SimpleStatsAggregator expected = new SimpleStatsAggregator();
        testCase(ft,
            iw -> {
                int numDocs = randomIntBetween(10, 50);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    int numValues = randomIntBetween(1, 5);
                    for (int j = 0; j < numValues; j++) {
                        long value = randomIntBetween(-100, 100);
                        doc.add(new SortedNumericDocValuesField("field", value));
                        expected.add(value);
                    }
                    iw.addDocument(doc);
                }
            },
            stats -> {
                assertEquals(expected.count, stats.getCount(), 0);
                assertEquals(expected.sum, stats.getSum(), TOLERANCE);
                assertEquals(expected.min, stats.getMin(), 0);
                assertEquals(expected.max, stats.getMax(), 0);
                assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifySummationOfDoubles(values, 15.3, 0.9, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySummationOfDoubles(values, sum, sum / n, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySummationOfDoubles(double[] values, double expectedSum,
                                          double expectedAvg, double delta) throws IOException {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        ft.setName("field");

        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (double value : values) {
            max = Math.max(max, value);
            min = Math.min(min, value);
        }
        double expectedMax = max;
        double expectedMin = min;
        testCase(ft,
            iw -> {
                for (double value : values) {
                    iw.addDocument(singleton(new NumericDocValuesField("field", NumericUtils.doubleToSortableLong(value))));
                }
            },
            stats -> {
                assertEquals(values.length, stats.getCount());
                assertEquals(expectedAvg, stats.getAvg(), delta);
                assertEquals(expectedSum, stats.getSum(), delta);
                assertEquals(expectedMax, stats.getMax(), 0d);
                assertEquals(expectedMin, stats.getMin(), 0d);
            }
        );
    }

    public void testCase(MappedFieldType ft,
                         CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                         Consumer<InternalStats> verify) throws IOException {
        try (Directory directory = newDirectory();
             RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            buildIndex.accept(indexWriter);
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                StatsAggregationBuilder aggBuilder = new StatsAggregationBuilder("my_agg").field("field");
                InternalStats stats = search(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                verify.accept(stats);
            }
        }
    }

    static class SimpleStatsAggregator {
        long count = 0;
        double min = Long.MAX_VALUE;
        double max = Long.MIN_VALUE;
        double sum = 0;

        void add(double value) {
            count ++;
            if (Double.compare(value, min) < 0) {
                min = value;
            }
            if (Double.compare(value, max) > 0) {
                max = value;
            }
            sum += value;
        }
    }
}
