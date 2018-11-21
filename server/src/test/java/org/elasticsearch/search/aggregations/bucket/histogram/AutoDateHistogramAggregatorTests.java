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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;

import java.io.IOException;
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

    private static final List<DateTime> DATES_WITH_TIME = Arrays.asList(
        new DateTime(2010, 3, 12, 1, 7, 45, DateTimeZone.UTC),
        new DateTime(2010, 4, 27, 3, 43, 34, DateTimeZone.UTC),
        new DateTime(2012, 5, 18, 4, 11, 0, DateTimeZone.UTC),
        new DateTime(2013, 5, 29, 5, 11, 31, DateTimeZone.UTC),
        new DateTime(2013, 10, 31, 8, 24, 5, DateTimeZone.UTC),
        new DateTime(2015, 2, 13, 13, 9, 32, DateTimeZone.UTC),
        new DateTime(2015, 6, 24, 13, 47, 43, DateTimeZone.UTC),
        new DateTime(2015, 11, 13, 16, 14, 34, DateTimeZone.UTC),
        new DateTime(2016, 3, 4, 17, 9, 50, DateTimeZone.UTC),
        new DateTime(2017, 12, 12, 22, 55, 46, DateTimeZone.UTC));

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
            histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testMatchAllDocs() throws IOException {
        testSearchCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
            histogram -> assertEquals(10, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
            histogram -> assertEquals(8, histogram.getBuckets().size())
        );
    }

    public void testSubAggregations() throws IOException {
        testSearchAndReduceCase(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD)
                .subAggregation(AggregationBuilders.stats("stats").field(DATE_FIELD)),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2010-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                Stats stats = bucket.getAggregations().get("stats");
                assertEquals("2010-03-12T01:07:45.000Z", stats.getMinAsString());
                assertEquals("2010-04-27T03:43:34.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());

                bucket = buckets.get(1);
                assertEquals("2011-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());

                bucket = buckets.get(2);
                assertEquals("2012-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMinAsString());
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());

                bucket = buckets.get(3);
                assertEquals("2013-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2013-05-29T05:11:31.000Z", stats.getMinAsString());
                assertEquals("2013-10-31T08:24:05.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());

                bucket = buckets.get(4);
                assertEquals("2014-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());

                bucket = buckets.get(5);
                assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2015-02-13T13:09:32.000Z", stats.getMinAsString());
                assertEquals("2015-11-13T16:14:34.000Z", stats.getMaxAsString());
                assertEquals(3L, stats.getCount());

                bucket = buckets.get(6);
                assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMinAsString());
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());

                bucket = buckets.get(7);
                assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMinAsString());
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
            });
    }

    public void testNoDocs() throws IOException {
        final List<DateTime> dates = Collections.emptyList();
        final Consumer<AutoDateHistogramAggregationBuilder> aggregation = agg -> agg.setNumBuckets(10).field(DATE_FIELD);

        testSearchCase(DEFAULT_QUERY, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(DEFAULT_QUERY, dates, aggregation,
            Assert::assertNull
        );
    }

    public void testAggregateWrongField() throws IOException {
        testBothCases(DEFAULT_QUERY, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(10).field("wrong_field"),
            histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testIntervalYear() throws IOException {
        final long start = new DateTime(DateTimeZone.UTC).withDate(2015, 1, 1).getMillis();
        final long end = new DateTime(DateTimeZone.UTC).withDate(2017, 12, 31).getMillis();
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
            }
        );
        testSearchAndReduceCase(rangeQuery, DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            histogram -> {
                final DateTime startDate = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
                final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(startDate, 3);
                expectedDocCount.put(startDate.plusYears(1), 1);
                expectedDocCount.put(startDate.plusYears(2), 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testIntervalMonth() throws IOException {
        final List<DateTime> datesForMonthInterval = Arrays.asList(
            new DateTime(2017, 1, 1, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 2, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 3, 4, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 3, 5, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 3, 6, 0, 0, 0, DateTimeZone.UTC));
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
                final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForMonthInterval.get(0).withDayOfMonth(1), 1);
                expectedDocCount.put(datesForMonthInterval.get(1).withDayOfMonth(1), 2);
                expectedDocCount.put(datesForMonthInterval.get(3).withDayOfMonth(1), 3);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
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
        final List<DateTime> datesForDayInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 2, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 2, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 5, 0, 0, 0, DateTimeZone.UTC));
        final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
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
            }
        );
    }

    public void testIntervalDayWithTZ() throws IOException {
        final List<DateTime> datesForDayInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 2, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 2, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 3, 0, 0, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 5, 0, 0, 0, DateTimeZone.UTC));
        testSearchCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)), histogram -> {
                final Map<String, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put("2017-01-31T23:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T23:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-02T23:00:00.000-01:00", 3);
                expectedDocCount.put("2017-02-04T23:00:00.000-01:00", 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKeyAsString(), 0).longValue(), bucket.getDocCount()));
            });
        testSearchAndReduceCase(DEFAULT_QUERY, datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)), histogram -> {
                final Map<String, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put("2017-01-31T00:00:00.000-01:00", 1);
                expectedDocCount.put("2017-02-01T00:00:00.000-01:00", 2);
                expectedDocCount.put("2017-02-02T00:00:00.000-01:00", 3);
                expectedDocCount.put("2017-02-04T00:00:00.000-01:00", 1);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(5, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKeyAsString(), 0).longValue(), bucket.getDocCount()));
            });
    }

    public void testIntervalHour() throws IOException {
        final List<DateTime> datesForHourInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 9, 2, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 35, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 10, 15, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 13, 6, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 14, 4, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 14, 5, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 15, 59, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 6, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 48, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 59, 0, DateTimeZone.UTC));
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
                final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForHourInterval.get(0).withMinuteOfHour(0), 2);
                expectedDocCount.put(datesForHourInterval.get(2).withMinuteOfHour(0), 1);
                expectedDocCount.put(datesForHourInterval.get(3).withMinuteOfHour(0), 1);
                expectedDocCount.put(datesForHourInterval.get(4).withMinuteOfHour(0), 2);
                expectedDocCount.put(datesForHourInterval.get(6).withMinuteOfHour(0), 1);
                expectedDocCount.put(datesForHourInterval.get(7).withMinuteOfHour(0), 3);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
        testSearchAndReduceCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
            histogram -> {
                final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForHourInterval.get(0).withMinuteOfHour(0), 3);
                expectedDocCount.put(datesForHourInterval.get(0).plusHours(3).withMinuteOfHour(0), 3);
                expectedDocCount.put(datesForHourInterval.get(0).plusHours(6).withMinuteOfHour(0), 4);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testIntervalHourWithTZ() throws IOException {
        final List<DateTime> datesForHourInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 9, 2, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 35, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 10, 15, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 13, 6, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 14, 4, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 14, 5, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 15, 59, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 6, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 48, 0, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 16, 59, 0, DateTimeZone.UTC));
        testSearchCase(DEFAULT_QUERY, datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)),
            histogram -> {
                final List<String> dateStrings = datesForHourInterval.stream()
                    .map(dateTime -> dateTime.withZone(DateTimeZone.forOffsetHours(-1)).toString()).collect(Collectors.toList());
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
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)),
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
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusSeconds(i);
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
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusMinutes(i);
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
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusHours(i);
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
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusDays(i);
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
                    assertEquals(startDate.plusMonths(randomIndex).dayOfMonth().getMaximumValue(), bucket.getDocCount());
                });
        }
    }

    public void testRandomMonthIntervals() throws IOException {
        final int length = 60;
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusMonths(i);
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
        final List<DateTime> dataset = new ArrayList<>(length);
        final DateTime startDate = new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC);
        for (int i = 0; i < length; i++) {
            final DateTime date = startDate.plusYears(i);
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
        final List<DateTime> datesForMinuteInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 9, 2, 35, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 2, 59, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 15, 37, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 16, 4, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 9, 16, 42, DateTimeZone.UTC));

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
                final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
                expectedDocCount.put(datesForMinuteInterval.get(0).withSecondOfMinute(0), 2);
                expectedDocCount.put(datesForMinuteInterval.get(2).withSecondOfMinute(0), 1);
                expectedDocCount.put(datesForMinuteInterval.get(3).withSecondOfMinute(0), 2);
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(15, buckets.size());
                buckets.forEach(bucket ->
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            }
        );
    }

    public void testIntervalSecond() throws IOException {
        final List<DateTime> datesForSecondInterval = Arrays.asList(
            new DateTime(2017, 2, 1, 0, 0, 5, 15, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 0, 0, 7, 299, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 0, 0, 7, 74, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 0, 0, 11, 688, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 0, 0, 11, 210, DateTimeZone.UTC),
            new DateTime(2017, 2, 1, 0, 0, 11, 380, DateTimeZone.UTC));
        final DateTime startDate = datesForSecondInterval.get(0).withMillisOfSecond(0);
        final Map<DateTime, Integer> expectedDocCount = new HashMap<>();
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

    private void testSearchCase(final Query query, final List<DateTime> dataset,
                                final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                final Consumer<Histogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }

    private void testSearchAndReduceCase(final Query query, final List<DateTime> dataset,
                                         final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                         final Consumer<Histogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
    }

    private void testBothCases(final Query query, final List<DateTime> dataset,
                               final Consumer<AutoDateHistogramAggregationBuilder> configure,
                               final Consumer<Histogram> verify) throws IOException {
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

    private void executeTestCase(final boolean reduced, final Query query, final List<DateTime> dataset,
                                 final Consumer<AutoDateHistogramAggregationBuilder> configure,
                                 final Consumer<Histogram> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                final Document document = new Document();
                for (final DateTime date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    final long instant = date.getMillis();
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
