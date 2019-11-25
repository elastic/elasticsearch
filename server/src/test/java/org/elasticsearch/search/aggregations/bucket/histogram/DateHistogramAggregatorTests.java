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
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.TooManyBucketsException;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class DateHistogramAggregatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "date";
    private static final String INSTANT_FIELD = "instant";

    private static final List<String> dataset = Arrays.asList(
            "2010-03-12T01:07:45",
            "2010-04-27T03:43:34",
            "2012-05-18T04:11:00",
            "2013-05-29T05:11:31",
            "2013-10-31T08:24:05",
            "2015-02-13T13:09:32",
            "2015-06-24T13:47:43",
            "2015-11-13T16:14:34",
            "2016-03-04T17:09:50",
            "2017-12-12T22:55:46");

    public void testMatchNoDocsDeprecatedInterval() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> {
                    assertEquals(0, histogram.getBuckets().size());
                    assertFalse(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
    }

    public void testMatchAllDocsDeprecatedInterval() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> {
                    assertEquals(6, histogram.getBuckets().size());
                    assertTrue(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        testSearchAndReduceCase(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> {
                    assertEquals(8, histogram.getBuckets().size());
                    assertTrue(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        testBothCases(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD).minDocCount(1L),
                histogram -> {
                    assertEquals(6, histogram.getBuckets().size());
                    assertTrue(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        List<String> foo = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            foo.add(dataset.get(randomIntBetween(0, dataset.size()-1)));
        }
        testSearchAndReduceCase(query, foo,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD).order(BucketOrder.count(false)),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );

        testSearchCase(query, dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );
        testSearchAndReduceCase(query, dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD).minDocCount(1L),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );

        testSearchCase(query, dataset,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );
        testSearchAndReduceCase(query, dataset,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD).minDocCount(1L),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );
    }

    public void testNoDocsDeprecatedInterval() throws IOException {
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<DateHistogramAggregationBuilder> aggregation = agg ->
                agg.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD);

        testSearchCase(query, dates, aggregation,
                histogram -> {
                    assertEquals(0, histogram.getBuckets().size());
                    assertFalse(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        testSearchAndReduceCase(query, dates, aggregation,
                histogram -> assertNull(histogram), false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testNoDocs() throws IOException {
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<DateHistogramAggregationBuilder> aggregation = agg ->
            agg.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD);
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchAndReduceCase(query, dates, aggregation,
            histogram -> assertNull(histogram), false
        );

        aggregation = agg ->
            agg.fixedInterval(new DateHistogramInterval("365d")).field(DATE_FIELD);
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchAndReduceCase(query, dates, aggregation,
            histogram -> assertNull(histogram), false
        );
    }

    public void testAggregateWrongFieldDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field("wrong_field"),
                histogram -> {
                    assertEquals(0, histogram.getBuckets().size());
                    assertFalse(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testAggregateWrongField() throws IOException {
        testBothCases(new MatchAllDocsQuery(), dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field("wrong_field"),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testBothCases(new MatchAllDocsQuery(), dataset,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field("wrong_field"),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
    }

    public void testIntervalYearDeprecated() throws IOException {
        testBothCases(LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2015-01-01"), asLong("2017-12-31")), dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalYear() throws IOException {
        testBothCases(LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2015-01-01"), asLong("2017-12-31")), dataset,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }, false
        );
    }

    public void testIntervalMonthDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
                Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.MONTH).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-03-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalMonth() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.MONTH).field(DATE_FIELD),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-03-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
    }

    public void testIntervalDayDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01",
                        "2017-02-02",
                        "2017-02-02",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-05"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.DAY).field(DATE_FIELD).minDocCount(1L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(4, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-02T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-03T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-05T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalDay() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.DAY).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(4, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-02T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-03T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());

                bucket = buckets.get(3);
                assertEquals("2017-02-05T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }, false
        );
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("24h")).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(4, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-02T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-03T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());

                bucket = buckets.get(3);
                assertEquals("2017-02-05T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }, false
        );
    }

    public void testIntervalHourDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T09:02:00.000Z",
                        "2017-02-01T09:35:00.000Z",
                        "2017-02-01T10:15:00.000Z",
                        "2017-02-01T13:06:00.000Z",
                        "2017-02-01T14:04:00.000Z",
                        "2017-02-01T14:05:00.000Z",
                        "2017-02-01T15:59:00.000Z",
                        "2017-02-01T16:06:00.000Z",
                        "2017-02-01T16:48:00.000Z",
                        "2017-02-01T16:59:00.000Z"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.HOUR).field(DATE_FIELD).minDocCount(1L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(6, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T10:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T13:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T14:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T15:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T16:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalHour() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:00.000Z",
                "2017-02-01T09:35:00.000Z",
                "2017-02-01T10:15:00.000Z",
                "2017-02-01T13:06:00.000Z",
                "2017-02-01T14:04:00.000Z",
                "2017-02-01T14:05:00.000Z",
                "2017-02-01T15:59:00.000Z",
                "2017-02-01T16:06:00.000Z",
                "2017-02-01T16:48:00.000Z",
                "2017-02-01T16:59:00.000Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.HOUR).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(6, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T09:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T10:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T13:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(3);
                assertEquals("2017-02-01T14:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(4);
                assertEquals("2017-02-01T15:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(5);
                assertEquals("2017-02-01T16:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:00.000Z",
                "2017-02-01T09:35:00.000Z",
                "2017-02-01T10:15:00.000Z",
                "2017-02-01T13:06:00.000Z",
                "2017-02-01T14:04:00.000Z",
                "2017-02-01T14:05:00.000Z",
                "2017-02-01T15:59:00.000Z",
                "2017-02-01T16:06:00.000Z",
                "2017-02-01T16:48:00.000Z",
                "2017-02-01T16:59:00.000Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("60m")).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(6, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T09:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T10:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T13:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(3);
                assertEquals("2017-02-01T14:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(4);
                assertEquals("2017-02-01T15:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(5);
                assertEquals("2017-02-01T16:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
    }

    public void testIntervalMinuteDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T09:02:35.000Z",
                        "2017-02-01T09:02:59.000Z",
                        "2017-02-01T09:15:37.000Z",
                        "2017-02-01T09:16:04.000Z",
                        "2017-02-01T09:16:42.000Z"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.MINUTE).field(DATE_FIELD).minDocCount(1L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:15:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T09:16:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalMinute() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:35.000Z",
                "2017-02-01T09:02:59.000Z",
                "2017-02-01T09:15:37.000Z",
                "2017-02-01T09:16:04.000Z",
                "2017-02-01T09:16:42.000Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.MINUTE).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T09:15:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T09:16:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }, false
        );
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:35.000Z",
                "2017-02-01T09:02:59.000Z",
                "2017-02-01T09:15:37.000Z",
                "2017-02-01T09:16:04.000Z",
                "2017-02-01T09:16:42.000Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("60s")).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T09:15:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T09:16:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }, false
        );
    }

    public void testIntervalSecondDeprecated() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T00:00:05.015Z",
                        "2017-02-01T00:00:11.299Z",
                        "2017-02-01T00:00:11.074Z",
                        "2017-02-01T00:00:37.688Z",
                        "2017-02-01T00:00:37.210Z",
                        "2017-02-01T00:00:37.380Z"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.SECOND).field(DATE_FIELD).minDocCount(1L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T00:00:37.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIntervalSecond() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:37.688Z",
                "2017-02-01T00:00:37.210Z",
                "2017-02-01T00:00:37.380Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.SECOND).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T00:00:37.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:37.688Z",
                "2017-02-01T00:00:37.210Z",
                "2017-02-01T00:00:37.380Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("1000ms")).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T00:00:37.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
    }

    public void testNanosIntervalSecond() throws IOException {
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015298384Z",
                "2017-02-01T00:00:11.299954583Z",
                "2017-02-01T00:00:11.074986434Z",
                "2017-02-01T00:00:37.688314602Z",
                "2017-02-01T00:00:37.210328172Z",
                "2017-02-01T00:00:37.380889483Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.SECOND).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T00:00:37.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, true
        );
        testBothCases(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015298384Z",
                "2017-02-01T00:00:11.299954583Z",
                "2017-02-01T00:00:11.074986434Z",
                "2017-02-01T00:00:37.688314602Z",
                "2017-02-01T00:00:37.210328172Z",
                "2017-02-01T00:00:37.380889483Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("1000ms")).field(DATE_FIELD).minDocCount(1L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(3, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T00:00:37.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, true
        );
    }

    public void testMinDocCountDeprecated() throws IOException {
        Query query = LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2017-02-01T00:00:00.000Z"), asLong("2017-02-01T00:00:30.000Z"));
        List<String> timestamps = Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:13.688Z",
                "2017-02-01T00:00:21.380Z"
        );

        // 5 sec interval with minDocCount = 0
        testSearchAndReduceCase(query, timestamps,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(0L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(4, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T00:00:15.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T00:00:20.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }, false
        );

        // 5 sec interval with minDocCount = 3
        testSearchAndReduceCase(query, timestamps,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(3L),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(1, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testMinDocCount() throws IOException {
        Query query = LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2017-02-01T00:00:00.000Z"), asLong("2017-02-01T00:00:30.000Z"));
        List<String> timestamps = Arrays.asList(
            "2017-02-01T00:00:05.015Z",
            "2017-02-01T00:00:11.299Z",
            "2017-02-01T00:00:11.074Z",
            "2017-02-01T00:00:13.688Z",
            "2017-02-01T00:00:21.380Z"
        );

        // 5 sec interval with minDocCount = 0
        testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(0L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(4, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());

                bucket = buckets.get(1);
                assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());

                bucket = buckets.get(2);
                assertEquals("2017-02-01T00:00:15.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());

                bucket = buckets.get(3);
                assertEquals("2017-02-01T00:00:20.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }, false
        );

        // 5 sec interval with minDocCount = 3
        testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(3L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(1, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
    }

    public void testMaxBucket() throws IOException {
        Query query = new MatchAllDocsQuery();
        List<String> timestamps = Arrays.asList(
            "2010-01-01T00:00:00.000Z",
            "2011-01-01T00:00:00.000Z",
            "2017-01-01T00:00:00.000Z"
        );

        expectThrows(TooManyBucketsException.class, () -> testSearchCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD),
            histogram -> {}, 2, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD),
            histogram -> {}, 2, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(0L),
            histogram -> {}, 100, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation ->
                aggregation.fixedInterval(DateHistogramInterval.seconds(5))
                    .field(DATE_FIELD)
                    .subAggregation(
                        AggregationBuilders.dateHistogram("1")
                            .fixedInterval(DateHistogramInterval.seconds(5))
                            .field(DATE_FIELD)
                    ),
            histogram -> {}, 5, false));
    }

    public void testMaxBucketDeprecated() throws IOException {
        Query query = new MatchAllDocsQuery();
        List<String> timestamps = Arrays.asList(
            "2010-01-01T00:00:00.000Z",
            "2011-01-01T00:00:00.000Z",
            "2017-01-01T00:00:00.000Z"
        );

        expectThrows(TooManyBucketsException.class, () -> testSearchCase(query, timestamps,
            aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD),
            histogram -> {}, 2, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD),
            histogram -> {}, 2, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(DATE_FIELD).minDocCount(0L),
            histogram -> {}, 100, false));

        expectThrows(TooManyBucketsException.class, () -> testSearchAndReduceCase(query, timestamps,
            aggregation ->
                aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5))
                    .field(DATE_FIELD)
                    .subAggregation(
                        AggregationBuilders.dateHistogram("1")
                            .dateHistogramInterval(DateHistogramInterval.seconds(5))
                            .field(DATE_FIELD)
                    ),
            histogram -> {}, 5, false));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testFixedWithCalendar() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.WEEK).field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("failed to parse setting [date_histogram.fixedInterval] with value [1w] as a time value: " +
            "unit is missing or unrecognized"));
    }

    public void testCalendarWithFixed() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(new DateHistogramInterval("5d")).field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("The supplied interval [5d] could not be parsed as a calendar interval."));
    }

    public void testCalendarAndThenFixed() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.DAY)
                .fixedInterval(new DateHistogramInterval("2d"))
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [fixed_interval] with [calendar_interval] configuration option."));
    }

    public void testFixedAndThenCalendar() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("2d"))
                .calendarInterval(DateHistogramInterval.DAY)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [calendar_interval] with [fixed_interval] configuration option."));
    }

    public void testNewThenLegacy() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("2d"))
                .dateHistogramInterval(DateHistogramInterval.DAY)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [interval] with [fixed_interval] or [calendar_interval] configuration options."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.DAY)
                .dateHistogramInterval(DateHistogramInterval.DAY)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [interval] with [fixed_interval] or [calendar_interval] configuration options."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("2d"))
                .interval(1000)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [interval] with [fixed_interval] or [calendar_interval] configuration options."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.DAY)
                .interval(1000)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [interval] with [fixed_interval] or [calendar_interval] configuration options."));
    }

    public void testLegacyThenNew() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation .dateHistogramInterval(DateHistogramInterval.DAY)
                .fixedInterval(new DateHistogramInterval("2d"))
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [fixed_interval] with [interval] configuration option."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.DAY)
                .calendarInterval(DateHistogramInterval.DAY)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [calendar_interval] with [interval] configuration option."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.interval(1000)
                .fixedInterval(new DateHistogramInterval("2d"))
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [fixed_interval] with [interval] configuration option."));

        e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.interval(1000)
                .calendarInterval(DateHistogramInterval.DAY)
                .field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [calendar_interval] with [interval] configuration option."));

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testIllegalInterval() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Collections.emptyList(),
            aggregation -> aggregation.dateHistogramInterval(new DateHistogramInterval("foobar")).field(DATE_FIELD),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Unable to parse interval [foobar]"));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    private void testSearchCase(Query query, List<String> dataset,
                                Consumer<DateHistogramAggregationBuilder> configure,
                                Consumer<InternalDateHistogram> verify, boolean useNanosecondResolution) throws IOException {
        testSearchCase(query, dataset, configure, verify, 10000, useNanosecondResolution);
    }

    private void testSearchCase(Query query, List<String> dataset,
                                Consumer<DateHistogramAggregationBuilder> configure,
                                Consumer<InternalDateHistogram> verify,
                                int maxBucket, boolean useNanosecondResolution) throws IOException {
        executeTestCase(false, query, dataset, configure, verify, maxBucket, useNanosecondResolution);
    }

    private void testSearchAndReduceCase(Query query, List<String> dataset,
                                         Consumer<DateHistogramAggregationBuilder> configure,
                                         Consumer<InternalDateHistogram> verify, boolean useNanosecondResolution) throws IOException {
        testSearchAndReduceCase(query, dataset, configure, verify, 1000, useNanosecondResolution);
    }

    private void testSearchAndReduceCase(Query query, List<String> dataset,
                                         Consumer<DateHistogramAggregationBuilder> configure,
                                         Consumer<InternalDateHistogram> verify,
                                         int maxBucket, boolean useNanosecondResolution) throws IOException {
        executeTestCase(true, query, dataset, configure, verify, maxBucket, useNanosecondResolution);
    }

    private void testBothCases(Query query, List<String> dataset,
                               Consumer<DateHistogramAggregationBuilder> configure,
                               Consumer<InternalDateHistogram> verify, boolean useNanosecondResolution) throws IOException {
        testBothCases(query, dataset, configure, verify, 10000, useNanosecondResolution);
    }

    private void testBothCases(Query query, List<String> dataset,
                               Consumer<DateHistogramAggregationBuilder> configure,
                               Consumer<InternalDateHistogram> verify,
                               int maxBucket, boolean useNanosecondResolution) throws IOException {
        testSearchCase(query, dataset, configure, verify, maxBucket, useNanosecondResolution);
        testSearchAndReduceCase(query, dataset, configure, verify, maxBucket, useNanosecondResolution);
    }

    private void executeTestCase(boolean reduced,
                                 Query query,
                                 List<String> dataset,
                                 Consumer<DateHistogramAggregationBuilder> configure,
                                 Consumer<InternalDateHistogram> verify,
                                 int maxBucket, boolean useNanosecondResolution) throws IOException {

        try (Directory directory = newDirectory()) {
            DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
            if (useNanosecondResolution) {
                builder.withResolution(DateFieldMapper.Resolution.NANOSECONDS);
            }
            DateFieldMapper.DateFieldType fieldType = builder.fieldType();
            fieldType.setHasDocValues(true);

            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    long instant = asLong(date, fieldType);
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new LongPoint(INSTANT_FIELD, instant));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                fieldType.setName(aggregationBuilder.field());

                InternalDateHistogram histogram;
                if (reduced) {
                    histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, maxBucket, null, fieldType);
                } else {
                    histogram = search(indexSearcher, query, aggregationBuilder, maxBucket, fieldType);
                }
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    private static long asLong(String dateTime, DateFieldMapper.DateFieldType fieldType) {
        return fieldType.parse(dateTime);
    }
}
