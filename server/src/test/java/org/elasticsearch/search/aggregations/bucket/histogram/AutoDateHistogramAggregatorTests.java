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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

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
                histogram -> assertEquals(6, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
                histogram -> assertEquals(8, histogram.getBuckets().size())
        );
    }

    public void testSubAggregations() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset,
                aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD)
                        .subAggregation(AggregationBuilders.stats("stats").field(DATE_FIELD)),
                histogram -> {
                    List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                    assertEquals(6, buckets.size());

                    Histogram.Bucket bucket = buckets.get(0);
                    assertEquals("2010-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());
                    Stats stats = bucket.getAggregations().get("stats");
                    assertEquals("2010-03-12T01:07:45.000Z", stats.getMinAsString());
                    assertEquals("2010-04-27T03:43:34.000Z", stats.getMaxAsString());
                    assertEquals(2L, stats.getCount());

                    bucket = buckets.get(1);
                    assertEquals("2012-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                    stats = bucket.getAggregations().get("stats");
                    assertEquals("2012-05-18T04:11:00.000Z", stats.getMinAsString());
                    assertEquals("2012-05-18T04:11:00.000Z", stats.getMaxAsString());
                    assertEquals(1L, stats.getCount());

                    bucket = buckets.get(2);
                    assertEquals("2013-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(2, bucket.getDocCount());
                    stats = bucket.getAggregations().get("stats");
                    assertEquals("2013-05-29T05:11:31.000Z", stats.getMinAsString());
                    assertEquals("2013-10-31T08:24:05.000Z", stats.getMaxAsString());
                    assertEquals(2L, stats.getCount());

                    bucket = buckets.get(3);
                    assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(3, bucket.getDocCount());
                    stats = bucket.getAggregations().get("stats");
                    assertEquals("2015-02-13T13:09:32.000Z", stats.getMinAsString());
                    assertEquals("2015-11-13T16:14:34.000Z", stats.getMaxAsString());
                    assertEquals(3L, stats.getCount());

                    bucket = buckets.get(4);
                    assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                    stats = bucket.getAggregations().get("stats");
                    assertEquals("2016-03-04T17:09:50.000Z", stats.getMinAsString());
                    assertEquals("2016-03-04T17:09:50.000Z", stats.getMaxAsString());
                    assertEquals(1L, stats.getCount());

                    bucket = buckets.get(5);
                    assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                    stats = bucket.getAggregations().get("stats");
                    assertEquals("2017-12-12T22:55:46.000Z", stats.getMinAsString());
                    assertEquals("2017-12-12T22:55:46.000Z", stats.getMaxAsString());
                    assertEquals(1L, stats.getCount());
                });
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
        testBothCases(LongPoint.newRangeQuery(INSTANT_FIELD, asLong("2015-01-01"), asLong("2017-12-31")), dataset,
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
        testBothCases(new MatchAllDocsQuery(),
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
