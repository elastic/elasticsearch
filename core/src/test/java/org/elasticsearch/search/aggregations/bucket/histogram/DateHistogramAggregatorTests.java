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
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

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

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> assertEquals(6, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD),
                histogram -> assertEquals(8, histogram.getBuckets().size())
        );
        testBothCases(query, dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD).minDocCount(1L),
                histogram -> assertEquals(6, histogram.getBuckets().size())
        );
    }

    public void testNoDocs() throws IOException {
        Query query = new MatchNoDocsQuery();
        List<String> dates = Collections.emptyList();
        Consumer<DateHistogramAggregationBuilder> aggregation = agg ->
                agg.dateHistogramInterval(DateHistogramInterval.YEAR).field(DATE_FIELD);

        testSearchCase(query, dates, aggregation,
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
        testSearchAndReduceCase(query, dates, aggregation,
                histogram -> assertNull(histogram)
        );
    }

    public void testAggregateWrongField() throws IOException {
        testBothCases(new MatchAllDocsQuery(), dataset,
                aggregation -> aggregation.dateHistogramInterval(DateHistogramInterval.YEAR).field("wrong_field"),
                histogram -> assertEquals(0, histogram.getBuckets().size())
        );
    }

    public void testIntervalYear() throws IOException {
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
                }
        );
    }

    public void testIntervalMonth() throws IOException {
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
                }
        );
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
                }
        );
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
                }
        );
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
                }
        );
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
                }
        );
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
                }
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
                }
        );
    }

    private void testSearchCase(Query query, List<String> dataset,
                                Consumer<DateHistogramAggregationBuilder> configure,
                                Consumer<Histogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }

    private void testSearchAndReduceCase(Query query, List<String> dataset,
                                         Consumer<DateHistogramAggregationBuilder> configure,
                                         Consumer<Histogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
    }

    private void testBothCases(Query query, List<String> dataset,
                               Consumer<DateHistogramAggregationBuilder> configure,
                               Consumer<Histogram> verify) throws IOException {
        testSearchCase(query, dataset, configure, verify);
        testSearchAndReduceCase(query, dataset, configure, verify);
    }

    private void executeTestCase(boolean reduced, Query query, List<String> dataset,
                                 Consumer<DateHistogramAggregationBuilder> configure,
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

                DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
                DateFieldMapper.DateFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(aggregationBuilder.field());

                InternalDateHistogram histogram;
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
