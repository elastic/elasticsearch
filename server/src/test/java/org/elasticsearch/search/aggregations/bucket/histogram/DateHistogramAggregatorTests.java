/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.github.nik9000.mapmatcher.ListMatcher.matchesList;
import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

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

    public void testBooleanFieldDeprecated() throws IOException {
        final String fieldName = "bogusBoolean";
        testCase(
            new DateHistogramAggregationBuilder("name").calendarInterval(DateHistogramInterval.HOUR).field(fieldName),
            new MatchAllDocsQuery(),
            iw -> {
                Document d = new Document();
                d.add(new SortedNumericDocValuesField(fieldName, 0));
                iw.addDocument(d);
            },
            a -> {},
            new BooleanFieldMapper.BooleanFieldType(fieldName)
        );
        assertWarnings("Running DateHistogram aggregations on [boolean] fields is deprecated");
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

    public void testFewRoundingPointsUsesFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyy")),
            IntStream.range(2000, 2010).mapToObj(Integer::toString).collect(toList()),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR),
            true
        );
    }

    public void testManyRoundingPointsDoesNotUseFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyy")),
            IntStream.range(2000, 3000).mapToObj(Integer::toString).collect(toList()),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR),
            false
        );
    }

    /**
     * Nanos doesn't use from range, but we don't get the fancy compile into
     * filters because of potential loss of precision.
     */
    public void testNanosDoesUseFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(true, true, DateFormatter.forPattern("yyyy")),
            List.of("2017", "2018"),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR),
            true
        );
    }

    public void testFarFutureDoesNotUseFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyyyy")),
            List.of("402017", "402018"),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR),
            false
        );
    }

    public void testMissingValueDoesNotUseFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyy")),
            List.of("2017", "2018"),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR).missing("2020"),
            false
        );
    }

    public void testExtendedBoundsUsesFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyy")),
            List.of("2017", "2018"),
            List.of("2016", "2017", "2018", "2019"),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE)
                .calendarInterval(DateHistogramInterval.YEAR)
                .extendedBounds(new LongBounds("2016", "2019"))
                .minDocCount(0),
            true
        );
    }

    public void testHardBoundsUsesFromRange() throws IOException {
        aggregationImplementationChoiceTestCase(
            aggregableDateFieldType(false, true, DateFormatter.forPattern("yyyy")),
            List.of("2016", "2017", "2018", "2019"),
            List.of("2017", "2018"),
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE)
                .calendarInterval(DateHistogramInterval.YEAR)
                .hardBounds(new LongBounds("2017", "2019")),
            true
        );
    }

    public void testOneBucketOptimized() throws IOException {
        AggregationBuilder builder = new DateHistogramAggregationBuilder("d").field("f").calendarInterval(DateHistogramInterval.DAY);
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            long start = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01T00:00:01");
            for (int i = 0; i < RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS; i++) {
                long date = start + i;
                iw.addDocument(List.of(new LongPoint("f", date), new NumericDocValuesField("f", date)));
            }
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of());
            }
        };
        DateFieldMapper.DateFieldType ft = new DateFieldMapper.DateFieldType("f");
        // Exists queries convert to MatchNone if this isn't defined
        FieldNamesFieldMapper.FieldNamesFieldType fnft = new FieldNamesFieldMapper.FieldNamesFieldType(true);
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalDateHistogram result, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(result.getBuckets(), hasSize(1));
                assertThat(result.getBuckets().get(0).getKeyAsString(), equalTo("2020-01-01T00:00:00.000Z"));
                assertThat(result.getBuckets().get(0).getDocCount(), equalTo(5000L));

                assertThat(impl, equalTo(DateHistogramAggregator.FromDateRange.class));
                assertMap(debug, matchesMap()
                    .entry("d", matchesMap()
                        .entry("delegate", "RangeAggregator.FromFilters")
                        .entry("delegate_debug", matchesMap()
                            .entry("ranges", 1)
                            .entry("average_docs_per_range", 5010.0)
                            .entry("delegate", "FilterByFilterAggregator")
                            .entry("delegate_debug", matchesMap()
                                .entry("segments_with_doc_count_field", 0)
                                .entry("segments_with_deleted_docs", 0)
                                .entry("segments_counted", greaterThan(0))
                                .entry("segments_collected", 0)
                                .entry("filters", matchesList().item(matchesMap()
                                    .entry("query", "DocValuesFieldExistsQuery [field=f]")
                                    .entry("specialized_for", "docvalues_field_exists")
                                    .entry("results_from_metadata", greaterThan(0)))
                                )
                            )
                        )
                    )
                );
            },
            ft,
            fnft
        );
    }

    private void aggregationImplementationChoiceTestCase(
        DateFieldMapper.DateFieldType ft,
        List<String> data,
        DateHistogramAggregationBuilder builder,
        boolean usesFromRange
    ) throws IOException {
        aggregationImplementationChoiceTestCase(ft, data, data, builder, usesFromRange);
    }

    private void aggregationImplementationChoiceTestCase(
        DateFieldMapper.DateFieldType ft,
        List<String> data,
        List<String> resultingBucketKeys,
        DateHistogramAggregationBuilder builder,
        boolean usesFromRange
    ) throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            for (String d : data) {
                long instant = asLong(d, ft);
                indexWriter.addDocument(
                    List.of(new SortedNumericDocValuesField(AGGREGABLE_DATE, instant), new LongPoint(AGGREGABLE_DATE, instant))
                );
            }
            try (IndexReader reader = indexWriter.getReader()) {
                AggregationContext context = createAggregationContext(new IndexSearcher(reader), new MatchAllDocsQuery(), ft);
                Aggregator agg = createAggregator(builder, context);
                Matcher<Aggregator> matcher = instanceOf(DateHistogramAggregator.FromDateRange.class);
                if (usesFromRange == false) {
                    matcher = not(matcher);
                }
                assertThat(agg, matcher);
                agg.preCollection();
                context.searcher().search(context.query(), agg);
                InternalDateHistogram result = (InternalDateHistogram) agg.buildTopLevel();
                result = (InternalDateHistogram) result.reduce(
                    List.of(result),
                    ReduceContext.forFinalReduction(context.bigArrays(), null, context.multiBucketConsumer(), PipelineTree.EMPTY)
                );
                assertThat(
                    result.getBuckets().stream().map(InternalDateHistogram.Bucket::getKeyAsString).collect(toList()),
                    equalTo(resultingBucketKeys)
                );
            }
        }
    }

    public void testIllegalInterval() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                new MatchAllDocsQuery(),
                Collections.emptyList(),
                aggregation -> aggregation.calendarInterval(new DateHistogramInterval("foobar")).field(AGGREGABLE_DATE),
                histogram -> {},
                false
            )
        );
        assertThat(e.getMessage(), equalTo("The supplied interval [foobar] could not be parsed as a calendar interval."));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                new MatchAllDocsQuery(),
                Collections.emptyList(),
                aggregation -> aggregation.fixedInterval(new DateHistogramInterval("foobar")).field(AGGREGABLE_DATE),
                histogram -> {},
                false
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "failed to parse setting [date_histogram.fixedInterval] with value [foobar] as a time value:"
                    + " unit is missing or unrecognized"
            )
        );
    }

    public void testBuildEmpty() throws IOException {
        withAggregator(
            new DateHistogramAggregationBuilder("test").field(AGGREGABLE_DATE).calendarInterval(DateHistogramInterval.YEAR).offset(10),
            new MatchAllDocsQuery(),
            iw -> {},
            (searcher, aggregator) -> {
                InternalDateHistogram histo = (InternalDateHistogram) aggregator.buildEmptyAggregation();
                /*
                 * There was a time where we including the offset in the
                 * rounding in the emptyBucketInfo which would cause us to
                 * include the offset twice. This verifies that we don't do
                 * that.
                 */
                assertThat(histo.emptyBucketInfo.rounding.prepareForUnknown().round(0), equalTo(0L));
            },
            aggregableDateFieldType(false, true)
        );
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
