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

public class ExtendedStatsAggregatorTests extends AggregatorTestCase {
    private static final double TOLERANCE = 1e-5;

    // TODO: Add script test cases.  Should fail with defaultValuesSourceType() commented out.

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
                assertEquals(Double.NaN, stats.getVariance(), 0);
                assertEquals(Double.NaN, stats.getStdDeviation(), 0);
                assertEquals(0d, stats.getSumOfSquares(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testRandomDoubles() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        ft.setName("field");
        final ExtendedSimpleStatsAggregator expected = new ExtendedSimpleStatsAggregator();
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
                assertEquals(expected.sumOfSqrs, stats.getSumOfSquares(), TOLERANCE);
                assertEquals(expected.stdDev(), stats.getStdDeviation(), TOLERANCE);
                assertEquals(expected.variance(), stats.getVariance(), TOLERANCE);
                assertEquals(expected.stdDevBound(ExtendedStats.Bounds.LOWER, stats.getSigma()),
                    stats.getStdDeviationBound(ExtendedStats.Bounds.LOWER), TOLERANCE);
                assertEquals(expected.stdDevBound(ExtendedStats.Bounds.UPPER, stats.getSigma()),
                    stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    /**
     * Testcase for https://github.com/elastic/elasticsearch/issues/37303
     */
    public void testVarianceNonNegative() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        ft.setName("field");
        final ExtendedSimpleStatsAggregator expected = new ExtendedSimpleStatsAggregator();
        testCase(ft,
            iw -> {
                int numDocs = 3;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    double value = 49.95d;
                    long valueAsLong = NumericUtils.doubleToSortableLong(value);
                    doc.add(new SortedNumericDocValuesField("field", valueAsLong));
                    expected.add(value);
                    iw.addDocument(doc);
                }
            },
            stats -> {
                //since the value(49.95) is a constant, variance should be 0
                assertEquals(0.0d, stats.getVariance(), TOLERANCE);
                assertEquals(0.0d, stats.getStdDeviation(), TOLERANCE);
            }
        );
    }

    public void testRandomLongs() throws IOException {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        final ExtendedSimpleStatsAggregator expected = new ExtendedSimpleStatsAggregator();
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
                assertEquals(expected.sumOfSqrs, stats.getSumOfSquares(), TOLERANCE);
                assertEquals(expected.stdDev(), stats.getStdDeviation(), TOLERANCE);
                assertEquals(expected.variance(), stats.getVariance(), TOLERANCE);
                assertEquals(expected.stdDevBound(ExtendedStats.Bounds.LOWER, stats.getSigma()),
                    stats.getStdDeviationBound(ExtendedStats.Bounds.LOWER), TOLERANCE);
                assertEquals(expected.stdDevBound(ExtendedStats.Bounds.UPPER, stats.getSigma()),
                    stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testSummationAccuracy() throws IOException {
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifyStatsOfDoubles(values, 13.5, 16.21, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        double sumOfSqrs = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
            sumOfSqrs += values[i] * values[i];
        }
        verifyStatsOfDoubles(values, sum, sumOfSqrs, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyStatsOfDoubles(largeValues, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyStatsOfDoubles(largeValues, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0d);
    }

    private void verifyStatsOfDoubles(double[] values, double expectedSum,
                                      double expectedSumOfSqrs, double delta) throws IOException {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        final String fieldName = "field";
        ft.setName(fieldName);
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
                    iw.addDocument(singleton(new NumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong(value))));
                }
            },
            stats -> {
                assertEquals(values.length, stats.getCount());
                assertEquals(expectedSum / values.length, stats.getAvg(), delta);
                assertEquals(expectedSum, stats.getSum(), delta);
                assertEquals(expectedSumOfSqrs, stats.getSumOfSquares(), delta);
                assertEquals(expectedMax, stats.getMax(), 0d);
                assertEquals(expectedMin, stats.getMin(), 0d);
            }
        );
    }

    public void testCase(MappedFieldType ft,
                         CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                         Consumer<InternalExtendedStats> verify) throws IOException {
        try (Directory directory = newDirectory();
             RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            buildIndex.accept(indexWriter);
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                ExtendedStatsAggregationBuilder aggBuilder = new ExtendedStatsAggregationBuilder("my_agg")
                    .field("field")
                    .sigma(randomDoubleBetween(0, 10, true));
                InternalExtendedStats stats = search(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                verify.accept(stats);
            }
        }
    }

    static class ExtendedSimpleStatsAggregator extends StatsAggregatorTests.SimpleStatsAggregator {
        double sumOfSqrs = 0;

        void add(double value) {
            super.add(value);
            sumOfSqrs += (value * value);
        }

        double stdDev() {
            return Math.sqrt(variance());
        }

        double stdDevBound(ExtendedStats.Bounds bounds, double sigma) {
            if (bounds == ExtendedStats.Bounds.UPPER) {
                return (sum / count) + (Math.sqrt(variance()) * sigma);
            } else {
                return (sum / count) - (Math.sqrt(variance()) * sigma);
            }
        }

        double variance() {
            double variance = (sumOfSqrs - ((sum * sum) / count)) / count;
            return variance < 0  ? 0 : variance;
        }
    }
}
