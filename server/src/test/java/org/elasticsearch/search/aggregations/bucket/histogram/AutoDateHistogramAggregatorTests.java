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
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;

public class AutoDateHistogramAggregatorTests extends AggregatorTestCase {

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

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
                histogram -> assertEquals(10, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
                histogram -> assertEquals(8, histogram.getBuckets().size())
        );
    }

    public void testSubAggregations() throws IOException {
        Query query = new MatchAllDocsQuery();
        testSearchAndReduceCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD)
                        .subAggregation(AggregationBuilders.stats("stats").field(DATE_FIELD)),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
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
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<AutoDateHistogramAggregationBuilder> aggregation = agg -> agg.setNumBuckets(10).field(DATE_FIELD);

        testSearchCase(query, dates, aggregation,
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(query, dates, aggregation,
                histogram -> assertNull(histogram)
        );
    }

    public void testAggregateWrongField() throws IOException {
        testBothCases(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(10).field("wrong_field"),
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testIntervalYear() throws IOException {
        testSearchCase(LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2015-01-01"), asLong("2017-12-31")), dataset,
                aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(5, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2015-02-13T13:09:32.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2015-06-24T13:47:43.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2015-11-13T16:14:34.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2016-03-04T17:09:50.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-12-12T22:55:46.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
        testSearchAndReduceCase(LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2015-01-01"), asLong("2017-12-31")), dataset,
                aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
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
                }
        );
    }

    public void testIntervalMonth() throws IOException {
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
                aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD), histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(6, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-02T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-03T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-03-04T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-03-05T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-03-06T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-01-01", "2017-02-02", "2017-02-03", "2017-03-04", "2017-03-05", "2017-03-06"),
                aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
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
                }
        );
    }

    public void testWithLargeNumberOfBuckets() {
        Query query = new MatchAllDocsQuery();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> testSearchCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS+1).field(DATE_FIELD),
                // since an exception is thrown, this assertion won't be invoked.
                histogram -> assertTrue(false)
            ));
        assertThat(exception.getMessage(), containsString("must be less than"));
    }

    public void testIntervalDay() throws IOException {
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-02-01", "2017-02-02", "2017-02-02", "2017-02-03", "2017-02-03", "2017-02-03", "2017-02-05"),
                aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD), histogram -> {
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
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01",
                        "2017-02-02",
                        "2017-02-02",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-03",
                        "2017-02-05"
                ),
                aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(5, buckets.size());

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
                    assertEquals("2017-02-04T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-05T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
    }

    public void testIntervalDayWithTZ() throws IOException {
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-02-01", "2017-02-02", "2017-02-02", "2017-02-03", "2017-02-03", "2017-02-03", "2017-02-05"),
                aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)), histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(4, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-01-31T23:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T23:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-02T23:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-04T23:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-02-01", "2017-02-02", "2017-02-02", "2017-02-03", "2017-02-03", "2017-02-03", "2017-02-05"),
                aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)), histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(5, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-01-31T00:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-02T00:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-03T00:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-04T00:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                });
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
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(10, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:35:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T10:15:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T13:06:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T14:04:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T14:05:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T15:59:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T16:06:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(8);
                    assertEquals("2017-02-01T16:48:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(9);
                    assertEquals("2017-02-01T16:59:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
        testSearchAndReduceCase(new MatchAllDocsQuery(),
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
                aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(8, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T10:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T11:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T12:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T13:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T14:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T15:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T16:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }
        );
    }

    public void testIntervalHourWithTZ() throws IOException {
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
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(10, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T08:02:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T08:35:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T09:15:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T12:06:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T13:04:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T13:05:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T14:59:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T15:06:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(8);
                    assertEquals("2017-02-01T15:48:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(9);
                    assertEquals("2017-02-01T15:59:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
        testSearchAndReduceCase(new MatchAllDocsQuery(),
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
                aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD).timeZone(DateTimeZone.forOffsetHours(-1)),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(8, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T08:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T10:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T11:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T12:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T13:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T14:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T15:00:00.000-01:00", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }
        );
    }

    public void testAllSecondIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 600; i++) {
            DateTime date = startDate.plusSeconds(i);
            dataset.add(format.print(date));
        }

        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(600).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(600, buckets.size());
                    for (int i = 0; i < 600; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusSeconds(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });

        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(120, buckets.size());
                    for (int i = 0; i < 120; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusSeconds(i * 5), bucket.getKey());
                        assertEquals(5, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(100).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(60, buckets.size());
                    for (int i = 0; i < 60; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusSeconds(i * 10), bucket.getKey());
                        assertEquals(10, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(50).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(20, buckets.size());
                    for (int i = 0; i < 20; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusSeconds(i * 30), bucket.getKey());
                        assertEquals(30, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(15).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(10, buckets.size());
                    for (int i = 0; i < 10; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMinutes(i), bucket.getKey());
                        assertEquals(60, bucket.getDocCount());
                    }
                });
    }

    public void testAllMinuteIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 600; i++) {
            DateTime date = startDate.plusMinutes(i);
            dataset.add(format.print(date));
        }
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(600).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(600, buckets.size());
                    for (int i = 0; i < 600; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMinutes(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(120, buckets.size());
                    for (int i = 0; i < 120; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMinutes(i * 5), bucket.getKey());
                        assertEquals(5, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(100).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(60, buckets.size());
                    for (int i = 0; i < 60; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMinutes(i * 10), bucket.getKey());
                        assertEquals(10, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(50).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(20, buckets.size());
                    for (int i = 0; i < 20; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMinutes(i * 30), bucket.getKey());
                        assertEquals(30, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(15).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(10, buckets.size());
                    for (int i = 0; i < 10; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusHours(i), bucket.getKey());
                        assertEquals(60, bucket.getDocCount());
                    }
                });
    }

    public void testAllHourIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 600; i++) {
            DateTime date = startDate.plusHours(i);
            dataset.add(format.print(date));
        }
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(600).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(600, buckets.size());
                    for (int i = 0; i < 600; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusHours(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(200, buckets.size());
                    for (int i = 0; i < 200; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusHours(i * 3), bucket.getKey());
                        assertEquals(3, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(100).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(50, buckets.size());
                    for (int i = 0; i < 50; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusHours(i * 12), bucket.getKey());
                        assertEquals(12, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(30).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(25, buckets.size());
                    for (int i = 0; i < 25; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusDays(i), bucket.getKey());
                        assertEquals(24, bucket.getDocCount());
                    }
                });
    }

    public void testAllDayIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 700; i++) {
            DateTime date = startDate.plusDays(i);
            dataset.add(format.print(date));
        }
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(700).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(700, buckets.size());
                    for (int i = 0; i < 700; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusDays(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(100, buckets.size());
                    for (int i = 0; i < 100; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusDays(i * 7), bucket.getKey());
                        assertEquals(7, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(30).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(24, buckets.size());
                    for (int i = 0; i < 24; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMonths(i), bucket.getKey());
                        assertThat(bucket.getDocCount(), Matchers.lessThanOrEqualTo(31L));
                    }
                });
    }

    public void testAllMonthIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 600; i++) {
            DateTime date = startDate.plusMonths(i);
            dataset.add(format.print(date));
        }
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(600).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(600, buckets.size());
                    for (int i = 0; i < 600; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMonths(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(200, buckets.size());
                    for (int i = 0; i < 200; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusMonths(i * 3), bucket.getKey());
                        assertEquals(3, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.setNumBuckets(60).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(50, buckets.size());
                    for (int i = 0; i < 50; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i), bucket.getKey());
                        assertEquals(12, bucket.getDocCount());
                    }
                });
    }

    public void testAllYearIntervals() throws IOException {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        List<String> dataset = new ArrayList<>();
        DateTime startDate = new DateTime(2017, 01, 01, 00, 00, 00, ISOChronology.getInstanceUTC());
        for (int i = 0; i < 600; i++) {
            DateTime date = startDate.plusYears(i);
            dataset.add(format.print(date));
        }
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(600).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(600, buckets.size());
                    for (int i = 0; i < 600; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i), bucket.getKey());
                        assertEquals(1, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(300).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(120, buckets.size());
                    for (int i = 0; i < 120; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i * 5), bucket.getKey());
                        assertEquals(5, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(100).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(60, buckets.size());
                    for (int i = 0; i < 60; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i * 10), bucket.getKey());
                        assertEquals(10, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(50).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(30, buckets.size());
                    for (int i = 0; i < 30; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i * 20), bucket.getKey());
                        assertEquals(20, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(20).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(12, buckets.size());
                    for (int i = 0; i < 12; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i * 50), bucket.getKey());
                        assertEquals(50, bucket.getDocCount());
                    }
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(), dataset, aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(6, buckets.size());
                    for (int i = 0; i < 6; i++) {
                        Histogram.Bucket bucket = buckets.get(i);
                        assertEquals(startDate.plusYears(i * 100), bucket.getKey());
                        assertEquals(100, bucket.getDocCount());
                    }
                });
    }

    public void testInterval3Hour() throws IOException {
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
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(10, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:35:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T10:15:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T13:06:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T14:04:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T14:05:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T15:59:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T16:06:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(8);
                    assertEquals("2017-02-01T16:48:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(9);
                    assertEquals("2017-02-01T16:59:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
        testSearchAndReduceCase(new MatchAllDocsQuery(),
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
                aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T12:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T15:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(4, bucket.getDocCount());
                }
        );
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
                aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(5, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:02:35.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:02:59.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T09:15:37.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T09:16:04.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T09:16:42.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
        );
        testSearchAndReduceCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T09:02:35.000Z",
                        "2017-02-01T09:02:59.000Z",
                        "2017-02-01T09:15:37.000Z",
                        "2017-02-01T09:16:04.000Z",
                        "2017-02-01T09:16:42.000Z"
                ),
                aggregation -> aggregation.setNumBuckets(15).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(15, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T09:02:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T09:03:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T09:04:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T09:05:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T09:06:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T09:07:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T09:08:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(7);
                    assertEquals("2017-02-01T09:09:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(8);
                    assertEquals("2017-02-01T09:10:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(9);
                    assertEquals("2017-02-01T09:11:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(10);
                    assertEquals("2017-02-01T09:12:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(11);
                    assertEquals("2017-02-01T09:13:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(12);
                    assertEquals("2017-02-01T09:14:00.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(13);
                    assertEquals("2017-02-01T09:15:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(14);
                    assertEquals("2017-02-01T09:16:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());
                }
        );
    }

    public void testIntervalSecond() throws IOException {
        testSearchCase(new MatchAllDocsQuery(),
                Arrays.asList("2017-02-01T00:00:05.015Z", "2017-02-01T00:00:07.299Z", "2017-02-01T00:00:07.074Z",
                        "2017-02-01T00:00:11.688Z", "2017-02-01T00:00:11.210Z", "2017-02-01T00:00:11.380Z"),
                aggregation -> aggregation.setNumBuckets(7).field(DATE_FIELD), histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(3, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:07.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                });
        testSearchAndReduceCase(new MatchAllDocsQuery(),
                Arrays.asList(
                        "2017-02-01T00:00:05.015Z",
                        "2017-02-01T00:00:07.299Z",
                        "2017-02-01T00:00:07.074Z",
                        "2017-02-01T00:00:11.688Z",
                        "2017-02-01T00:00:11.210Z",
                        "2017-02-01T00:00:11.380Z"
                ),
                aggregation -> aggregation.setNumBuckets(7).field(DATE_FIELD),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(7, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2017-02-01T00:00:05.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());

                    bucket = buckets.get(1);
                    assertEquals("2017-02-01T00:00:06.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(2);
                    assertEquals("2017-02-01T00:00:07.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());

                    bucket = buckets.get(3);
                    assertEquals("2017-02-01T00:00:08.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(4);
                    assertEquals("2017-02-01T00:00:09.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-02-01T00:00:10.000Z", bucket.getKeyAsString());
                    assertEquals(0, bucket.getDocCount());

                    bucket = buckets.get(6);
                    assertEquals("2017-02-01T00:00:11.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                }
        );
    }

    private void testSearchCase(Query query, List<String> dataset,
            Consumer<AutoDateHistogramAggregationBuilder> configure,
                                Consumer<Histogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }

    private void testSearchAndReduceCase(Query query, List<String> dataset,
            Consumer<AutoDateHistogramAggregationBuilder> configure,
                                         Consumer<Histogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
    }

    private void testBothCases(Query query, List<String> dataset,
            Consumer<AutoDateHistogramAggregationBuilder> configure,
                               Consumer<Histogram> verify) throws IOException {
        testSearchCase(query, dataset, configure, verify);
        testSearchAndReduceCase(query, dataset, configure, verify);
    }

    @Override
    protected IndexSettings createIndexSettings() {
        Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", 100000).build();
        return new IndexSettings(
            IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(boolean reduced, Query query, List<String> dataset,
                                 Consumer<AutoDateHistogramAggregationBuilder> configure,
                                 Consumer<Histogram> verify) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    long instant = asLong(date);
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new LongPoint(INSTANT_FIELD, instant));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                AutoDateHistogramAggregationBuilder aggregationBuilder = new AutoDateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
                DateFieldMapper.DateFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(aggregationBuilder.field());

                InternalAutoDateHistogram histogram;
                if (reduced) {
                    histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                } else {
                    histogram = search(indexSearcher, query, aggregationBuilder, fieldType);
                }
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(dateTime).getMillis();
    }
}
