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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class DateRangeHistogramAggregatorTests extends AggregatorTestCase {

    public static final String FIELD_NAME = "fieldName";

    public void testBasics() throws Exception {
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T12:14:36"),
            asLong("2019-08-01T15:07:22"), true, true);
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY),
            writer -> writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range))))),
            histo -> {
                assertEquals(1, histo.getBuckets().size());
                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testFormat() throws Exception {
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T12:14:36"),
            asLong("2019-08-01T15:07:22"), true, true);
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY).format("yyyy-MM-dd"),
            writer -> writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range))))),
            histo -> {
                assertEquals(1, histo.getBuckets().size());
                assertTrue(AggregationInspectionHelper.hasValue(histo));

                assertEquals("2019-08-01", histo.getBuckets().get(0).getKeyAsString());
            }
        );
    }

    public void testUnsupportedRangeType() throws Exception {
        RangeType rangeType = RangeType.LONG;
        final String fieldName = "field";

        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange =
                rangeType.encodeRanges(singleton(new RangeFieldMapper.Range(rangeType, 12234, 89765, true, true)));
            doc.add(new BinaryDocValuesField(fieldName, encodedRange));
            w.addDocument(doc);

            DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("my_agg")
                .field(fieldName)
                .calendarInterval(DateHistogramInterval.MONTH);

            MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                expectThrows(IllegalArgumentException.class, () -> createAggregator(aggBuilder, searcher, fieldType));
            }
        }
    }

    /*
     * Test calendar interval behaves correctly on months over 30 days
     */
    public void testLongMonthsCalendarInterval() throws Exception {
        RangeFieldMapper.Range julyRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T00:00:00"),
            asLong("2019-07-31T23:59:59"), true, true);
        RangeFieldMapper.Range augustRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T00:00:00"),
            asLong("2019-08-31T23:59:59"), true, true);
        RangeFieldMapper.Range septemberRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-09-01T00:00:00"),
            asLong("2019-09-30T23:59:59"), true, true);

        // Calendar interval case - three months, three bucketLong.MIN_VALUE;s
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.MONTH),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(julyRange)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(augustRange)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(septemberRange)))));
            },
            histo -> {
                assertEquals(3, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T00:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-01T00:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-09-01T00:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    /*
     * Test fixed interval 30d behaves correctly with months over 30 days
     */
    public void testLongMonthsFixedInterval() throws Exception {
        RangeFieldMapper.Range julyRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T00:00:00"),
            asLong("2019-07-31T23:59:59"), true, true);
        RangeFieldMapper.Range augustRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T00:00:00"),
            asLong("2019-08-31T23:59:59"), true, true);
        RangeFieldMapper.Range septemberRange = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-09-01T00:00:00"),
            asLong("2019-09-30T23:59:59"), true, true);

        // Fixed interval case - 4 periods of 30 days
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.fixedInterval(new DateHistogramInterval("30d")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(julyRange)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(augustRange)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(septemberRange)))));
            },
            histo -> {
                assertEquals(4, histo.getBuckets().size());

                assertEquals(asZDT("2019-06-13T00:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-13T00:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(2, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-08-12T00:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(2, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-09-11T00:00:00"), histo.getBuckets().get(3).getKey());
                assertEquals(1, histo.getBuckets().get(3).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testOffsetCalendarInterval() throws Exception {

        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:15:00"),
            asLong("2019-07-01T03:20:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:45:00"),
            asLong("2019-07-01T03:50:00"), true, true);
        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:55:00"),
            asLong("2019-07-01T04:05:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T04:17:00"),
            asLong("2019-07-01T04:19:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T04:55:00"),
            asLong("2019-07-01T05:05:00"), true, true);

        // No offset, just to make sure the ranges line up as expected
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
            },
            histo -> {
                assertEquals(3, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(3, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(3, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-01T05:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );

        // 10 minute offset should shift all data into one bucket
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).offset("10m"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
            },
            histo -> {
                assertEquals(2, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:10:00"), histo.getBuckets().get(0).getKey());
                assertEquals(3, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:10:00"), histo.getBuckets().get(1).getKey());
                assertEquals(2, histo.getBuckets().get(1).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testOffsetFixedInterval() throws Exception {

        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:15:00"),
            asLong("2019-07-01T03:20:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:45:00"),
            asLong("2019-07-01T03:50:00"), true, true);
        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:55:00"),
            asLong("2019-07-01T04:05:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T04:17:00"),
            asLong("2019-07-01T04:19:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T04:55:00"),
            asLong("2019-07-01T05:05:00"), true, true);

        // No offset, just to make sure the ranges line up as expected
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.fixedInterval(new DateHistogramInterval("1h")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
            },
            histo -> {
                assertEquals(3, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(3, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(3, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-01T05:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );

        // 10 minute offset should shift all data into one bucket
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.fixedInterval(new DateHistogramInterval("1h")).offset("10m"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
            },
            histo -> {
                assertEquals(2, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:10:00"), histo.getBuckets().get(0).getKey());
                assertEquals(3, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:10:00"), histo.getBuckets().get(1).getKey());
                assertEquals(2, histo.getBuckets().get(1).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    /*
     * Test that when incrementing the rounded bucket key, offsets are correctly taken into account at the <1hour scale
     */
    public void testNextRoundingValueOffsetHours() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:15:00"),
            asLong("2019-07-01T03:20:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T04:15:00"),
            asLong("2019-07-01T04:20:00"), true, true);
        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T05:15:00"),
            asLong("2019-07-01T05:20:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T06:15:00"),
            asLong("2019-07-01T06:20:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T07:15:00"),
            asLong("2019-07-01T07:20:00"), true, true);
        RangeFieldMapper.Range range6 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T08:15:00"),
            asLong("2019-07-01T08:20:00"), true, true);

        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.fixedInterval(new DateHistogramInterval("1h")).offset("13m"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
            },
            histo -> {
                assertEquals(6, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:13:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:13:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-01T05:13:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-07-01T06:13:00"), histo.getBuckets().get(3).getKey());
                assertEquals(1, histo.getBuckets().get(3).getDocCount());

                assertEquals(asZDT("2019-07-01T07:13:00"), histo.getBuckets().get(4).getKey());
                assertEquals(1, histo.getBuckets().get(4).getDocCount());

                assertEquals(asZDT("2019-07-01T08:13:00"), histo.getBuckets().get(5).getKey());
                assertEquals(1, histo.getBuckets().get(5).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );

        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).offset("13m"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
            },
            histo -> {
                assertEquals(6, histo.getBuckets().size());

                assertEquals(asZDT("2019-07-01T03:13:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T04:13:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-01T05:13:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-07-01T06:13:00"), histo.getBuckets().get(3).getKey());
                assertEquals(1, histo.getBuckets().get(3).getDocCount());

                assertEquals(asZDT("2019-07-01T07:13:00"), histo.getBuckets().get(4).getKey());
                assertEquals(1, histo.getBuckets().get(4).getDocCount());

                assertEquals(asZDT("2019-07-01T08:13:00"), histo.getBuckets().get(5).getKey());
                assertEquals(1, histo.getBuckets().get(5).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    /*
     * Test that when incrementing the rounded bucket key, offsets are correctly taken into account when interval is on date scale and
     * offset is on time scale
     */
    public void testNextRoundingValueOffsetDays() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-01T03:15:00"),
            asLong("2019-07-01T03:20:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-02T04:15:00"),
            asLong("2019-07-02T04:20:00"), true, true);
        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-03T05:15:00"),
            asLong("2019-07-03T05:20:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-04T06:15:00"),
            asLong("2019-07-04T06:20:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-05T07:15:00"),
            asLong("2019-07-05T07:20:00"), true, true);
        RangeFieldMapper.Range range6 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-07-06T08:15:00"),
            asLong("2019-07-06T08:20:00"), true, true);

        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.fixedInterval(new DateHistogramInterval("1d")).offset("36h"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
            },
            histo -> {
                assertEquals(6, histo.getBuckets().size());

                assertEquals(asZDT("2019-06-30T12:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T12:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-02T12:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-07-03T12:00:00"), histo.getBuckets().get(3).getKey());
                assertEquals(1, histo.getBuckets().get(3).getDocCount());

                assertEquals(asZDT("2019-07-04T12:00:00"), histo.getBuckets().get(4).getKey());
                assertEquals(1, histo.getBuckets().get(4).getDocCount());

                assertEquals(asZDT("2019-07-05T12:00:00"), histo.getBuckets().get(5).getKey());
                assertEquals(1, histo.getBuckets().get(5).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );

        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY).offset("12h"),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
            },
            histo -> {
                assertEquals(6, histo.getBuckets().size());

                assertEquals(asZDT("2019-06-30T12:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-07-01T12:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-07-02T12:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-07-03T12:00:00"), histo.getBuckets().get(3).getKey());
                assertEquals(1, histo.getBuckets().get(3).getDocCount());

                assertEquals(asZDT("2019-07-04T12:00:00"), histo.getBuckets().get(4).getKey());
                assertEquals(1, histo.getBuckets().get(4).getDocCount());

                assertEquals(asZDT("2019-07-05T12:00:00"), histo.getBuckets().get(5).getKey());
                assertEquals(1, histo.getBuckets().get(5).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testMinDocCount() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T12:14:36"),
            asLong("2019-08-01T15:07:22"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T12:14:36"),
            asLong("2019-08-02T15:07:22"), true, true);
        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T12:14:36"),
            asLong("2019-08-02T15:07:22"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T12:14:36"),
            asLong("2019-08-03T15:07:22"), true, true);

        // Guard case, make sure the agg buckets as expected without min doc count
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
            },
            histo -> {
                assertEquals(3, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-01T00:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T00:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(3, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-08-03T00:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );

        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY).minDocCount(2),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
            },
            histo -> {
                assertEquals(1, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T00:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(3, histo.getBuckets().get(0).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testIntersectQuery() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T02:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);

        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T03:15:00"),
            asLong("2019-08-02T03:45:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T04:15:00"),
            asLong("2019-08-02T04:45:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T03:30:00"),
            asLong("2019-08-02T04:30:00"), true, true);

        RangeFieldMapper.Range range6 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T03:45:00"), true, true);
        RangeFieldMapper.Range range7 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T04:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range8 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:30:00"),
            asLong("2019-08-02T05:30:00"), true, true);

        Query query = RangeType.DATE.dvRangeQuery(FIELD_NAME, BinaryDocValuesRangeQuery.QueryType.INTERSECTS, asLong("2019-08-02T03:00:00"),
            asLong("2019-08-02T05:00:00"), true, true);


        testCase(
            query,
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).minDocCount(2),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range7)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range8)))));
            },
            histo -> {
                assertEquals(4, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T02:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(2, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T03:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(4, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-08-02T04:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(4, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-08-02T05:00:00"), histo.getBuckets().get(3).getKey());
                assertEquals(2, histo.getBuckets().get(3).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testWithinQuery() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T02:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);

        RangeFieldMapper.Range range3 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T03:15:00"),
            asLong("2019-08-02T03:45:00"), true, true);
        RangeFieldMapper.Range range4 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T04:15:00"),
            asLong("2019-08-02T04:45:00"), true, true);
        RangeFieldMapper.Range range5 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T03:30:00"),
            asLong("2019-08-02T04:30:00"), true, true);

        RangeFieldMapper.Range range6 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T03:45:00"), true, true);
        RangeFieldMapper.Range range7 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T04:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range8 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:30:00"),
            asLong("2019-08-02T05:30:00"), true, true);

        Query query = RangeType.DATE.dvRangeQuery(FIELD_NAME, BinaryDocValuesRangeQuery.QueryType.WITHIN, asLong("2019-08-02T03:00:00"),
            asLong("2019-08-02T05:00:00"), true, true);


        testCase(
            query,
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).minDocCount(2),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range3)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range4)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range5)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range6)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range7)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range8)))));
            },
            histo -> {
                assertEquals(2, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T03:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(2, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T04:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(2, histo.getBuckets().get(1).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testHardBounds() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            asLong("2019-08-02T17:45:00"), true, true);

        testCase(
            Queries.newMatchAllQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).hardBounds(
                new LongBounds("2019-08-02T03:00:00", "2019-08-02T10:00:00")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
            },
            histo -> {
                assertEquals(8, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T03:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T05:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(2, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-08-02T10:00:00"), histo.getBuckets().get(7).getKey());
                assertEquals(1, histo.getBuckets().get(7).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }
    public void testHardBoundsWithOpenRanges() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, Long.MIN_VALUE,
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            Long.MAX_VALUE, true, true);

        testCase(
            Queries.newMatchAllQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR).hardBounds(
                new LongBounds("2019-08-02T03:00:00", "2019-08-02T10:00:00")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
            },
            histo -> {
                assertEquals(8, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T03:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(1, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T05:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(2, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-08-02T10:00:00"), histo.getBuckets().get(7).getKey());
                assertEquals(1, histo.getBuckets().get(7).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testBothBounds() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            asLong("2019-08-02T17:45:00"), true, true);

        testCase(
            Queries.newMatchAllQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR)
                .hardBounds(new LongBounds("2019-08-02T00:00:00", "2019-08-02T10:00:00"))
                .extendedBounds(new LongBounds("2019-08-02T01:00:00", "2019-08-02T08:00:00")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
            },
            histo -> {
                assertEquals(10, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T01:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(0, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T02:00:00"), histo.getBuckets().get(1).getKey());
                assertEquals(1, histo.getBuckets().get(1).getDocCount());

                assertEquals(asZDT("2019-08-02T10:00:00"), histo.getBuckets().get(9).getKey());
                assertEquals(1, histo.getBuckets().get(9).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    public void testOverlappingBounds() throws Exception {

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> testCase(
            Queries.newMatchAllQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR)
                .hardBounds(new LongBounds("2019-08-02T01:00:00", "2019-08-02T08:00:00"))
                .extendedBounds(new LongBounds("2019-08-02T00:00:00", "2019-08-02T10:00:00")),
            writer -> {

            },
            histo -> {
               fail("Shouldn't be here");
            }
        ));

        assertThat(ex.getMessage(), equalTo("Extended bounds have to be inside hard bounds, " +
            "hard bounds: [2019-08-02T01:00:00--2019-08-02T08:00:00], extended bounds: [2019-08-02T00:00:00--2019-08-02T10:00:00]"));
    }

    public void testEqualBounds() throws Exception {
        RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T02:15:00"),
            asLong("2019-08-02T05:45:00"), true, true);
        RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-02T05:15:00"),
            asLong("2019-08-02T17:45:00"), true, true);

        testCase(
            Queries.newMatchAllQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.HOUR)
                .hardBounds(new LongBounds("2019-08-02T00:00:00", "2019-08-02T10:00:00"))
                .extendedBounds(new LongBounds("2019-08-02T00:00:00", "2019-08-02T10:00:00")),
            writer -> {
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range1)))));
                writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range2)))));
            },
            histo -> {
                assertEquals(11, histo.getBuckets().size());

                assertEquals(asZDT("2019-08-02T00:00:00"), histo.getBuckets().get(0).getKey());
                assertEquals(0, histo.getBuckets().get(0).getDocCount());

                assertEquals(asZDT("2019-08-02T02:00:00"), histo.getBuckets().get(2).getKey());
                assertEquals(1, histo.getBuckets().get(2).getDocCount());

                assertEquals(asZDT("2019-08-02T10:00:00"), histo.getBuckets().get(10).getKey());
                assertEquals(1, histo.getBuckets().get(10).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    private void testCase(Query query,
                          Consumer<DateHistogramAggregationBuilder> configure,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalDateHistogram> verify) throws IOException {
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(FIELD_NAME, RangeFieldMapper.Defaults.DATE_FORMATTER);
        final DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("_name").field(FIELD_NAME);
        if (configure != null) {
            configure.accept(aggregationBuilder);
        }
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(DateHistogramAggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalDateHistogram> verify,
                          MappedFieldType fieldType) throws IOException {
        try(Directory directory = newDirectory();
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalDateHistogram histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    private static ZonedDateTime asZDT(String dateTime) {
        return Instant.ofEpochMilli(asLong(dateTime)).atZone(ZoneOffset.UTC);
    }
}
