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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;

public class DateHistogramAggregatorTests extends DateHistogramAggregatorTestCase {
    /**
     * A date that is always "searchable" because it is indexed.
     */
    private static final String SEARCHABLE_DATE = "searchable_date";

    private static final List<String> DATASET = Arrays.asList(
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
        testSearchCase(new MatchNoDocsQuery(), DATASET,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
                histogram -> {
                    assertEquals(0, histogram.getBuckets().size());
                    assertFalse(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testMatchNoDocs() throws IOException {
        testSearchCase(new MatchNoDocsQuery(), DATASET,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchCase(new MatchNoDocsQuery(), DATASET,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(AGGREGABLE_DATE),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
    }

    public void testMatchAllDocsDeprecatedInterval() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, DATASET,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
                histogram -> {
                    assertEquals(8, histogram.getBuckets().size());
                    assertTrue(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        testSearchCase(query, DATASET,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE).minDocCount(1L),
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
            foo.add(DATASET.get(randomIntBetween(0, DATASET.size()-1)));
        }
        testSearchCase(query, foo,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d"))
                    .field(AGGREGABLE_DATE).order(BucketOrder.count(false)),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );

        testSearchCase(query, DATASET,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );
        testSearchCase(query, DATASET,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE).minDocCount(1L),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );

        testSearchCase(query, DATASET,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(AGGREGABLE_DATE),
            histogram -> assertEquals(8, histogram.getBuckets().size()), false
        );
        testSearchCase(query, DATASET,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field(AGGREGABLE_DATE).minDocCount(1L),
            histogram -> assertEquals(6, histogram.getBuckets().size()), false
        );
    }

    public void testAsSubAgg() throws IOException {
        AggregationBuilder builder = new TermsAggregationBuilder("k1").field("k1").subAggregation(
            new DateHistogramAggregationBuilder("dh").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR));
        asSubAggTestCase(builder, (StringTerms terms) -> {
            StringTerms.Bucket a = terms.getBucketByKey("a");
            InternalDateHistogram adh = a.getAggregations().get("dh");
            assertThat(adh.getBuckets().stream().map(bucket -> bucket.getKey().toString()).collect(toList()), equalTo(List.of(
                "2020-01-01T00:00Z", "2021-01-01T00:00Z"
            )));

            StringTerms.Bucket b = terms.getBucketByKey("b");
            InternalDateHistogram bdh = b.getAggregations().get("dh");
            assertThat(bdh.getBuckets().stream().map(bucket -> bucket.getKey().toString()).collect(toList()), equalTo(List.of(
                "2020-01-01T00:00Z"
            )));
        });
        builder = new TermsAggregationBuilder("k2").field("k2").subAggregation(builder);
        asSubAggTestCase(builder, (StringTerms terms) -> {
            StringTerms.Bucket a = terms.getBucketByKey("a");
            StringTerms ak1 = a.getAggregations().get("k1");
            StringTerms.Bucket ak1a = ak1.getBucketByKey("a");
            InternalDateHistogram ak1adh = ak1a.getAggregations().get("dh");
            assertThat(ak1adh.getBuckets().stream().map(bucket -> bucket.getKey().toString()).collect(toList()), equalTo(List.of(
                "2020-01-01T00:00Z", "2021-01-01T00:00Z"
            )));

            StringTerms.Bucket b = terms.getBucketByKey("b");
            StringTerms bk1 = b.getAggregations().get("k1");
            StringTerms.Bucket bk1a = bk1.getBucketByKey("a");
            InternalDateHistogram bk1adh = bk1a.getAggregations().get("dh");
            assertThat(bk1adh.getBuckets().stream().map(bucket -> bucket.getKey().toString()).collect(toList()), equalTo(List.of(
                "2021-01-01T00:00Z"
            )));
            StringTerms.Bucket bk1b = bk1.getBucketByKey("b");
            InternalDateHistogram bk1bdh = bk1b.getAggregations().get("dh");
            assertThat(bk1bdh.getBuckets().stream().map(bucket -> bucket.getKey().toString()).collect(toList()), equalTo(List.of(
                "2020-01-01T00:00Z"
            )));
        });
    }

    public void testNoDocsDeprecatedInterval() throws IOException {
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<DateHistogramAggregationBuilder> aggregation =
            agg -> agg.dateHistogramInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE);

        testSearchCase(query, dates, aggregation, histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        }, false);
        testSearchCase(query, dates, aggregation, histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        }, false);
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testNoDocs() throws IOException {
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<DateHistogramAggregationBuilder> aggregation = agg ->
            agg.calendarInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE);
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );

        aggregation = agg ->
            agg.fixedInterval(new DateHistogramInterval("365d")).field(AGGREGABLE_DATE);
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchCase(query, dates, aggregation,
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
    }

    public void testAggregateWrongFieldDeprecated() throws IOException {
        testSearchCase(new MatchAllDocsQuery(), DATASET,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field("wrong_field"),
                histogram -> {
                    assertEquals(0, histogram.getBuckets().size());
                    assertFalse(AggregationInspectionHelper.hasValue(histogram));
                }, false
        );
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }

    public void testAggregateWrongField() throws IOException {
        testSearchCase(new MatchAllDocsQuery(), DATASET,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field("wrong_field"),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
        testSearchCase(new MatchAllDocsQuery(), DATASET,
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("365d")).field("wrong_field"),
            histogram -> assertEquals(0, histogram.getBuckets().size()), false
        );
    }

    public void testIntervalYearDeprecated() throws IOException {
        testSearchCase(LongPoint.newRangeQuery(SEARCHABLE_DATE, asLong("2015-01-01"), asLong("2017-12-31")), DATASET,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
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
        testSearchCase(LongPoint.newRangeQuery(SEARCHABLE_DATE, asLong("2015-01-01"), asLong("2017-12-31")), DATASET,
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.YEAR).field(AGGREGABLE_DATE),
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
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.MONTH).field(AGGREGABLE_DATE),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.MONTH).field(AGGREGABLE_DATE),
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
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01",
                        "2017-02-02",
                        "2017-02-02",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-05"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.DAY).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.DAY).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("24h")).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
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
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.HOUR).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
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
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.HOUR).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
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
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("60m")).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T09:02:35.000Z",
                        "2017-02-01T09:02:59.000Z",
                        "2017-02-01T09:15:37.000Z",
                        "2017-02-01T09:16:04.000Z",
                        "2017-02-01T09:16:42.000Z"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.MINUTE).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:35.000Z",
                "2017-02-01T09:02:59.000Z",
                "2017-02-01T09:15:37.000Z",
                "2017-02-01T09:16:04.000Z",
                "2017-02-01T09:16:42.000Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.MINUTE).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T09:02:35.000Z",
                "2017-02-01T09:02:59.000Z",
                "2017-02-01T09:15:37.000Z",
                "2017-02-01T09:16:04.000Z",
                "2017-02-01T09:16:42.000Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("60s")).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T00:00:05.015Z",
                        "2017-02-01T00:00:11.299Z",
                        "2017-02-01T00:00:11.074Z",
                        "2017-02-01T00:00:37.688Z",
                        "2017-02-01T00:00:37.210Z",
                        "2017-02-01T00:00:37.380Z"
                ),
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.SECOND).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:37.688Z",
                "2017-02-01T00:00:37.210Z",
                "2017-02-01T00:00:37.380Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.SECOND).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:37.688Z",
                "2017-02-01T00:00:37.210Z",
                "2017-02-01T00:00:37.380Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("1000ms")).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015298384Z",
                "2017-02-01T00:00:11.299954583Z",
                "2017-02-01T00:00:11.074986434Z",
                "2017-02-01T00:00:37.688314602Z",
                "2017-02-01T00:00:37.210328172Z",
                "2017-02-01T00:00:37.380889483Z"
            ),
            aggregation -> aggregation.calendarInterval(DateHistogramInterval.SECOND).field(AGGREGABLE_DATE).minDocCount(1L),
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
        testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01T00:00:05.015298384Z",
                "2017-02-01T00:00:11.299954583Z",
                "2017-02-01T00:00:11.074986434Z",
                "2017-02-01T00:00:37.688314602Z",
                "2017-02-01T00:00:37.210328172Z",
                "2017-02-01T00:00:37.380889483Z"
            ),
            aggregation -> aggregation.fixedInterval(new DateHistogramInterval("1000ms")).field(AGGREGABLE_DATE).minDocCount(1L),
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
        Query query = LongPoint.newRangeQuery(SEARCHABLE_DATE, asLong("2017-02-01T00:00:00.000Z"), asLong("2017-02-01T00:00:30.000Z"));
        List<String> timestamps = Arrays.asList(
                "2017-02-01T00:00:05.015Z",
                "2017-02-01T00:00:11.299Z",
                "2017-02-01T00:00:11.074Z",
                "2017-02-01T00:00:13.688Z",
                "2017-02-01T00:00:21.380Z"
        );

        // 5 sec interval with minDocCount = 0
        testSearchCase(query, timestamps,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(AGGREGABLE_DATE).minDocCount(0L),
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
        testSearchCase(query, timestamps,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.seconds(5)).field(AGGREGABLE_DATE).minDocCount(3L),
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
        Query query = LongPoint.newRangeQuery(SEARCHABLE_DATE, asLong("2017-02-01T00:00:00.000Z"), asLong("2017-02-01T00:00:30.000Z"));
        List<String> timestamps = Arrays.asList(
            "2017-02-01T00:00:05.015Z",
            "2017-02-01T00:00:11.299Z",
            "2017-02-01T00:00:11.074Z",
            "2017-02-01T00:00:13.688Z",
            "2017-02-01T00:00:21.380Z"
        );

        // 5 sec interval with minDocCount = 0
        testSearchCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(AGGREGABLE_DATE).minDocCount(0L),
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
        testSearchCase(query, timestamps,
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.seconds(5)).field(AGGREGABLE_DATE).minDocCount(3L),
            histogram -> {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(1, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
            }, false
        );
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
            aggregation -> aggregation.fixedInterval(DateHistogramInterval.WEEK).field(AGGREGABLE_DATE),
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
            aggregation -> aggregation.calendarInterval(new DateHistogramInterval("5d")).field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
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
                .field(AGGREGABLE_DATE),
            histogram -> {}, false
        ));
        assertThat(e.getMessage(), equalTo("Cannot use [calendar_interval] with [interval] configuration option."));

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
    }


    public void testOverlappingBounds() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Arrays.asList(
                "2017-02-01",
                "2017-02-02",
                "2017-02-02",
                "2017-02-03",
                "2017-02-03",
                "2017-02-03",
                "2017-02-05"
            ),
            aggregation -> aggregation .calendarInterval(DateHistogramInterval.DAY)
                .hardBounds(new LongBounds("2010-01-01", "2020-01-01"))
                .extendedBounds(new LongBounds("2009-01-01", "2021-01-01"))
                .field(AGGREGABLE_DATE),
            histogram -> {}, false
        ));

        assertThat(ex.getMessage(), equalTo("Extended bounds have to be inside hard bounds, " +
            "hard bounds: [2010-01-01--2020-01-01], extended bounds: [2009-01-01--2021-01-01]"));
    }

    public void testIllegalInterval() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testSearchCase(new MatchAllDocsQuery(),
            Collections.emptyList(),
            aggregation -> aggregation.dateHistogramInterval(new DateHistogramInterval("foobar")).field(AGGREGABLE_DATE),
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
        boolean aggregableDateIsSearchable = randomBoolean();
        DateFieldMapper.DateFieldType fieldType = aggregableDateFieldType(useNanosecondResolution, aggregableDateIsSearchable);

        try (Directory directory = newDirectory()) {

            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String date : dataset) {
                    long instant = asLong(date, fieldType);
                    document.add(new SortedNumericDocValuesField(AGGREGABLE_DATE, instant));
                    if (aggregableDateIsSearchable) {
                        document.add(new LongPoint(AGGREGABLE_DATE, instant));
                    }
                    document.add(new LongPoint(SEARCHABLE_DATE, instant));
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

                InternalDateHistogram histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, maxBucket, fieldType);
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
