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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.hamcrest.Matchers;
import org.junit.Assert;

import java.io.IOException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AutoDateHistogramAggregatorTests extends AggregatorTestCase {
    private static final String DATE_FIELD = "date";
    private static final String INSTANT_FIELD = "instant";

    private static final List<ZonedDateTime> DATES_WITH_TIME = Arrays.asList(
        ZonedDateTime.of(2010, 3, 12, 1, 7, 45, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2010, 4, 27, 3, 43, 34, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2012, 5, 18, 4, 11, 0, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2013, 5, 29, 5, 11, 31, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2013, 10, 31, 8, 24, 5, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 2, 13, 13, 9, 32, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 6, 24, 13, 47, 43, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2016, 3, 4, 17, 9, 50, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2017, 12, 12, 22, 55, 46, 0, ZoneOffset.UTC));

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
            histogram -> {
                assertEquals(0, histogram.getBuckets().size());
                assertFalse(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testMatchAllDocs() throws IOException {
        testSearchCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
            histogram -> {
                assertEquals(10, histogram.getBuckets().size());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
            histogram -> {
                assertEquals(8, histogram.getBuckets().size());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testSubAggregations() throws IOException {
        testSearchAndReduceCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD)
                .subAggregation(AggregationBuilders.stats("stats").field(DATE_FIELD)),
            histogram -> {
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2010-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                InternalStats stats = bucket.getAggregations().get("stats");
                assertEquals("2010-03-12T01:07:45.000Z", stats.getMinAsString());
                assertEquals("2010-04-27T03:43:34.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(1);
                assertEquals("2011-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());
                assertFalse(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(2);
                assertEquals("2012-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMinAsString());
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(3);
                assertEquals("2013-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2013-05-29T05:11:31.000Z", stats.getMinAsString());
                assertEquals("2013-10-31T08:24:05.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(4);
                assertEquals("2014-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());
                assertFalse(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(5);
                assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2015-02-13T13:09:32.000Z", stats.getMinAsString());
                assertEquals("2015-11-13T16:14:34.000Z", stats.getMaxAsString());
                assertEquals(3L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(6);
                assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMinAsString());
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(7);
                assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMinAsString());
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            });
    }

    public void testNoDocs() throws IOException {
        final List<ZonedDateTime> dates = Collections.emptyList();
        final Consumer<AutoDateHistogramAggregationBuilder> aggregation = agg -> agg.setNumBuckets(10).field(DATE_FIELD);

        testSearchCase(DEFAULT_QUERY, dates, aggregation,
            histogram -> {
                assertEquals(0, histogram.getBuckets().size());
                assertFalse(AggregationInspectionHelper.hasValue(histogram));
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, dates, aggregation,
            Assert::assertNull
        );
    }

    public void testAggregateWrongField() throws IOException {
        testBothCases(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(10).field("wrong_field"),
            histogram -> {
                assertEquals(0, histogram.getBuckets().size());
                assertFalse(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testIntervalYear() throws IOException {


        final long start = LocalDate.of(2015, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final long end = LocalDate.of(2017, 12, 31).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final Query rangeQuery = LongPoint.newRangeQuery(INSTANT_FIELD, start, end);
        testSearchCase(rangeQuery, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(5, buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    final Histogram.Bucket bucket = buckets.get(i);
                    assertEquals(DATES_WITH_TIME.get(5 + i), bucket.getKey());
                    assertEquals(1, bucket.getDocCount());
                }
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
        testSearchAndReduceCase(rangeQuery, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            histogram -> {
                final ZonedDateTime startDate = ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
                final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(startDate, 3);
                expectedDocCount.put(startDate.plusYears(1), 1);
                expectedDocCount.put(startDate.plusYears(2), 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testIntervalMonth() throws IOException {
        final List<ZonedDateTime> datesForMonthInterval = Arrays.asList(
            ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 4, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 5, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 6, 0, 0, 0, 0, ZoneOffset.UTC));
        testSearchCase(DEFAULT_QUERY, datesForMonthInterval,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(datesForMonthInterval.size(), buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    final Histogram.Bucket bucket = buckets.get(i);
                    assertEquals(datesForMonthInterval.get(i), bucket.getKey());
                    assertEquals(1, bucket.getDocCount());
                }
            });
        testSearchAndReduceCase(DEFAULT_QUERY, datesForMonthInterval,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            histogram -> {
                final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForMonthInterval.get(0).withDayOfMonth(1), 1);
                expectedDocCount.put(datesForMonthInterval.get(1).withDayOfMonth(1), 2);
                expectedDocCount.put(datesForMonthInterval.get(3).withDayOfMonth(1), 3);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testWithLargeNumberOfBuckets() {
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> testSearchCase(DEFAULT_QUERY, DATES_WITH_TIME,
                aggregation -> aggregation.setNumBuckets(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1).field(DATE_FIELD),
                // since an exception is thrown, this assertion won't be invoked.
                histogram -> fail()
            ));
        assertThat(exception.getMessage(), Matchers.containsString("must be less than"));
    }

    public void testIntervalDay() throws IOException {
        final List<ZonedDateTime> datesForDayInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 5, 0, 0, 0, 0, ZoneOffset.UTC));
        final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(datesForDayInterval.get(0), 1);
        expectedDocCount.put(datesForDayInterval.get(1), 2);
        expectedDocCount.put(datesForDayInterval.get(3), 3);
        expectedDocCount.put(datesForDayInterval.get(6), 1);

        testSearchCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            });
        testSearchAndReduceCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(5, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testIntervalDayWithTZ() throws IOException {
        final List<ZonedDateTime> datesForDayInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 5, 0, 0, 0, 0, ZoneOffset.UTC));
        testSearchCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)), histogram -> {
                final Map<String, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put("2017-01-31T23:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T23:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-02T23:00:00.000-01:00", 3);
                expectedDocCount.put("2017-02-04T23:00:00.000-01:00", 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKeyAsString(), 0).longValue(), bucket.getDocCount()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            });
        testSearchAndReduceCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)), histogram -> {
                final Map<String, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put("2017-01-31T00:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T00:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-02T00:00:00.000-01:00", 3);
                expectedDocCount.put("2017-02-04T00:00:00.000-01:00", 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(5, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKeyAsString(), 0).longValue(), bucket.getDocCount()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            });
    }

    public void testIntervalHour() throws IOException {
        final List<ZonedDateTime> datesForHourInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 35, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 10, 15, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 13, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 4, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 5, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 15, 59, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 48, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 59, 0, 0, ZoneOffset.UTC));
        testSearchCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(datesForHourInterval.size(), buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    final Histogram.Bucket bucket = buckets.get(i);
                    assertEquals(datesForHourInterval.get(i), bucket.getKey());
                    assertEquals(1, bucket.getDocCount());
                }
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
            histogram -> {
                final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForHourInterval.get(0).withMinute(0), 2);
                expectedDocCount.put(datesForHourInterval.get(2).withMinute(0), 1);
                expectedDocCount.put(datesForHourInterval.get(3).withMinute(0), 1);
                expectedDocCount.put(datesForHourInterval.get(4).withMinute(0), 2);
                expectedDocCount.put(datesForHourInterval.get(6).withMinute(0), 1);
                expectedDocCount.put(datesForHourInterval.get(7).withMinute(0), 3);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
            histogram -> {
                final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForHourInterval.get(0).withMinute(0), 3);
                expectedDocCount.put(datesForHourInterval.get(0).plusHours(3).withMinute(0), 3);
                expectedDocCount.put(datesForHourInterval.get(0).plusHours(6).withMinute(0), 4);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testIntervalHourWithTZ() throws IOException {
        final List<ZonedDateTime> datesForHourInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 35, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 10, 15, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 13, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 4, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 5, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 15, 59, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 48, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 59, 0, 0, ZoneOffset.UTC));
        testSearchCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)),
            histogram -> {
                final List<String> dateStrings = datesForHourInterval.stream()
                    .map(dateTime -> DateFormatter.forPattern("strict_date_time")
                        .format(dateTime.withZoneSameInstant(ZoneOffset.ofHours(-1)))).collect(Collectors.toList());
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(datesForHourInterval.size(), buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    final Histogram.Bucket bucket = buckets.get(i);
                    assertEquals(dateStrings.get(i), bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)),
            histogram -> {
                final Map<String, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put("2017-02-01T08:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-01T09:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T12:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T13:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-01T14:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T15:00:00.000-01:00", 3);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKeyAsString(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testRandomSecondIntervals() throws IOException {
        final int length = 120;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusSeconds(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(120, 1);
        bucketsToExpectedDocCountMap.put(60, 5);
        bucketsToExpectedDocCountMap.put(20, 10);
        bucketsToExpectedDocCountMap.put(10, 30);
        bucketsToExpectedDocCountMap.put(3, 60);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusSeconds(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
    }

    public void testRandomMinuteIntervals() throws IOException {
        final int length = 120;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusMinutes(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(120, 1);
        bucketsToExpectedDocCountMap.put(60, 5);
        bucketsToExpectedDocCountMap.put(20, 10);
        bucketsToExpectedDocCountMap.put(10, 30);
        bucketsToExpectedDocCountMap.put(3, 60);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusMinutes(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
    }

    public void testRandomHourIntervals() throws IOException {
        final int length = 72;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusHours(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(72, 1);
        bucketsToExpectedDocCountMap.put(36, 3);
        bucketsToExpectedDocCountMap.put(12, 12);
        bucketsToExpectedDocCountMap.put(3, 24);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusHours(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
    }

    public void testRandomDayIntervals() throws IOException {
        final int length = 140;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusDays(i);
            dataset.add(date);
        }
        final int randomChoice = randomIntBetween(1, 3);
        if (randomChoice == 1) {
            testSearchAndReduceCase(DEFAULT_QUERY, dataset,
                aggregation -> aggregation.setNumBuckets(length).field(DATE_FIELD),
                histogram -> {
                    final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(length, buckets.size());
                    final int randomIndex = randomInt(length - 1);
                    final Histogram.Bucket bucket = buckets.get(randomIndex);
                    assertEquals(startDate.plusDays(randomIndex), bucket.getKey());
                    assertEquals(1, bucket.getDocCount());
                });
        } else if (randomChoice == 2) {
            testSearchAndReduceCase(DEFAULT_QUERY, dataset,
                aggregation -> aggregation.setNumBuckets(60).field(DATE_FIELD),
                histogram -> {
                    final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    final int expectedDocCount = 7;
                    assertEquals(20, buckets.size());
                    final int randomIndex = randomInt(19);
                    final Histogram.Bucket bucket = buckets.get(randomIndex);
                    assertEquals(startDate.plusDays(randomIndex * expectedDocCount), bucket.getKey());
                    assertEquals(expectedDocCount, bucket.getDocCount());
                });
        } else if (randomChoice == 3) {
            testSearchAndReduceCase(DEFAULT_QUERY, dataset,
                aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
                histogram -> {
                    final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(5, buckets.size());
                    final int randomIndex = randomInt(2);
                    final Histogram.Bucket bucket = buckets.get(randomIndex);
                    assertEquals(startDate.plusMonths(randomIndex), bucket.getKey());
                    assertEquals(YearMonth.from(startDate.plusMonths(randomIndex)).lengthOfMonth(), bucket.getDocCount());
                });
        }
    }

    public void testRandomMonthIntervals() throws IOException {
        final int length = 60;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusMonths(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(60, 1);
        bucketsToExpectedDocCountMap.put(30, 3);
        bucketsToExpectedDocCountMap.put(6, 12);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusMonths(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
    }

    public void testRandomYearIntervals() throws IOException {
        final int length = 300;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusYears(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(300, 1);
        bucketsToExpectedDocCountMap.put(150, 5);
        bucketsToExpectedDocCountMap.put(50, 10);
        bucketsToExpectedDocCountMap.put(25, 20);
        bucketsToExpectedDocCountMap.put(10, 50);
        bucketsToExpectedDocCountMap.put(5, 100);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusYears(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
    }

    public void testIntervalMinute() throws IOException {
        final List<ZonedDateTime> datesForMinuteInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 35, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 2, 59, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 15, 37, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 16, 4, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 16, 42, 0, ZoneOffset.UTC));

        testSearchCase(DEFAULT_QUERY, datesForMinuteInterval,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(datesForMinuteInterval.size(), buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    final Histogram.Bucket bucket = buckets.get(i);
                    assertEquals(datesForMinuteInterval.get(i), bucket.getKey());
                    assertEquals(1, bucket.getDocCount());
                }
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, datesForMinuteInterval,
            aggregation -> aggregation.setNumBuckets(15).field(DATE_FIELD),
            histogram -> {
                final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForMinuteInterval.get(0).withSecond(0), 2);
                expectedDocCount.put(datesForMinuteInterval.get(2).withSecond(0), 1);
                expectedDocCount.put(datesForMinuteInterval.get(3).withSecond(0), 2);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(15, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testIntervalSecond() throws IOException {
        final List<ZonedDateTime> datesForSecondInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 5, 15, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 7, 299, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 7, 74, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 688, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 210, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 380, ZoneOffset.UTC));
        final ZonedDateTime startDate = datesForSecondInterval.get(0).withNano(0);
        final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(startDate, 1);
        expectedDocCount.put(startDate.plusSeconds(2), 2);
        expectedDocCount.put(startDate.plusSeconds(6), 3);

        testSearchCase(DEFAULT_QUERY, datesForSecondInterval,
            aggregation -> aggregation.setNumBuckets(7).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            });
        testSearchAndReduceCase(DEFAULT_QUERY, datesForSecondInterval,
            aggregation -> aggregation.setNumBuckets(7).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(7, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    private void testSearchCase(final Query query, final List<ZonedDateTime> dataset,
                                final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                final Consumer<InternalAutoDateHistogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }

    private void testSearchAndReduceCase(final Query query, final List<ZonedDateTime> dataset,
                                         final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                         final Consumer<InternalAutoDateHistogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
    }

    private void testBothCases(final Query query, final List<ZonedDateTime> dataset,
                               final Consumer<AutoDateHistogramAggregationBuilder> configure,
                               final Consumer<InternalAutoDateHistogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
        executeTestCase(true, query, dataset, configure, verify);
    }

    @Override
    protected IndexSettings createIndexSettings() {
        final Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", 25000).build();
        return new IndexSettings(
            IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(final boolean reduced, final Query query, final List<ZonedDateTime> dataset,
                                 final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                 final Consumer<InternalAutoDateHistogram> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                final Document document = new Document();
                for (final ZonedDateTime date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    final long instant = date.toInstant().toEpochMilli();
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new LongPoint(INSTANT_FIELD, instant));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                final AutoDateHistogramAggregationBuilder aggregationBuilder = new AutoDateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                final DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
                final DateFieldMapper.DateFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(aggregationBuilder.field());

                final InternalAutoDateHistogram histogram;
                if (reduced) {
                    histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                } else {
                    histogram = search(indexSearcher, query, aggregationBuilder, fieldType);
                }
                verify.accept(histogram);
            }
        }
    }
}
