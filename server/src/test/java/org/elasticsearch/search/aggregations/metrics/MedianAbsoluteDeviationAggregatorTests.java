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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.ExactMedianAbsoluteDeviation.calculateMAD;
import static org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.IsCloseToRelative.closeToRelative;
import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationAggregatorTests extends AggregatorTestCase {

    private static final int SAMPLE_MIN = -1000000;
    private static final int SAMPLE_MAX = 1000000;

    private static <T extends IndexableField> CheckedConsumer<RandomIndexWriter, IOException> randomSample(
            int size,
            Function<Long, Iterable<T>> field) {

        return writer -> {
            for (int i = 0; i < size; i++) {
                final long point = randomLongBetween(SAMPLE_MIN, SAMPLE_MAX);
                Iterable<T> document = field.apply(point);
                writer.addDocument(document);
            }
        };
    }

    // intentionally not writing any docs
    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), writer -> {}, agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
            assertFalse(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            writer -> {
                writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
                writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 2)));
            },
            agg -> {
                assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
                assertFalse(AggregationInspectionHelper.hasValue(agg));
            }
        );
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testCase(
            new DocValuesFieldExistsQuery("number"),
            randomSample(size, point -> {
                sample.add(point);
                return singleton(new SortedNumericDocValuesField("number", point));
            }),
            agg -> {
                assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(sample)));
                assertTrue(AggregationInspectionHelper.hasValue(agg));
            }
        );
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testCase(
            new DocValuesFieldExistsQuery("number"),
            randomSample(size, point -> {
                sample.add(point);
                return singleton(new NumericDocValuesField("number", point));
            }),
            agg -> {
                assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(sample)));
                assertTrue(AggregationInspectionHelper.hasValue(agg));
            }
        );
    }

    public void testQueryFiltering() throws IOException {
        final int lowerRange = 1;
        final int upperRange = 500;
        final int[] sample = IntStream.rangeClosed(1, 1000).toArray();
        final int[] filteredSample = Arrays.stream(sample).filter(point -> point  >= lowerRange && point <= upperRange).toArray();
        testCase(
            IntPoint.newRangeQuery("number", lowerRange, upperRange),
            writer -> {
                for (int point : sample) {
                    writer.addDocument(Arrays.asList(new IntPoint("number", point), new SortedNumericDocValuesField("number", point)));
                }
            },
            agg -> {
                assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(filteredSample)));
                assertTrue(AggregationInspectionHelper.hasValue(agg));
            }
        );
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(
            IntPoint.newRangeQuery("number", -1, 0),
            writer -> {
                writer.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
                writer.addDocument(Arrays.asList(new IntPoint("number", 2), new SortedNumericDocValuesField("number", 2)));
            },
            agg -> {
                assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
                assertFalse(AggregationInspectionHelper.hasValue(agg));
            }
        );
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalMedianAbsoluteDeviation> verify) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                MedianAbsoluteDeviationAggregationBuilder builder = new MedianAbsoluteDeviationAggregationBuilder("mad")
                    .field("number")
                    .compression(randomDoubleBetween(20, 1000, true));

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType.setName("number");

                MedianAbsoluteDeviationAggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();

                verify.accept((InternalMedianAbsoluteDeviation) aggregator.buildAggregation(0L));
            }
        }

    }

    public static class IsCloseToRelative extends TypeSafeMatcher<Double> {

        private final double expected;
        private final double error;

        public IsCloseToRelative(double expected, double error) {
            this.expected = expected;
            this.error = error;
        }

        @Override
        protected boolean matchesSafely(Double actual) {
            final double deviation = Math.abs(actual - expected);
            final double observedError = deviation / Math.abs(expected);
            return observedError <= error;
        }

        @Override
        public void describeTo(Description description) {
            description
                .appendText("within ")
                .appendValue(error * 100)
                .appendText(" percent of ")
                .appendValue(expected);
        }

        public static IsCloseToRelative closeToRelative(double expected, double error) {
            return new IsCloseToRelative(expected, error);
        }

        public static IsCloseToRelative closeToRelative(double expected) {
            return closeToRelative(expected, 0.1);
        }
    }

    /**
     * This class is an implementation of median absolute deviation that computes an exact value, rather than the approximation used in the
     * aggregation. It's used to verify that the aggregation's approximate results are close enough to the exact result
     */
    public static class ExactMedianAbsoluteDeviation {

        public static double calculateMAD(int[] sample) {
            return calculateMAD(Arrays.stream(sample)
                .mapToDouble(point -> (double) point)
                .toArray());
        }

        public static double calculateMAD(long[] sample) {
            return calculateMAD(Arrays.stream(sample)
                .mapToDouble(point -> (double) point)
                .toArray());
        }

        public static double calculateMAD(List<Long> sample) {
            return calculateMAD(sample.stream()
                .mapToDouble(Long::doubleValue)
                .toArray());
        }

        public static double calculateMAD(double[] sample) {
            final double median = calculateMedian(sample);

            final double[] deviations = Arrays.stream(sample)
                .map(point -> Math.abs(median - point))
                .toArray();

            final double mad = calculateMedian(deviations);
            return mad;
        }

        private static double calculateMedian(double[] sample) {
            final double[] sorted = Arrays.copyOf(sample, sample.length);
            Arrays.sort(sorted);
            final int halfway = (int) Math.ceil(sorted.length / 2d);
            final double median;
            if (sorted.length % 2 == 0) {
                // even
                median = (sorted[halfway - 1] + sorted[halfway]) / 2d;
            } else {
                // odd
                median = (sorted[halfway - 1]);
            }
            return median;
        }

    }
}
