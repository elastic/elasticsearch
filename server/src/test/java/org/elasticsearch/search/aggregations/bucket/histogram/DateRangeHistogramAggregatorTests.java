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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
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

            MappedFieldType fieldType = new RangeFieldMapper.Builder(fieldName, rangeType).fieldType();
            fieldType.setName(fieldName);

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

    private void testCase(Query query,
                          Consumer<DateHistogramAggregationBuilder> configure,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalDateHistogram> verify) throws IOException {
        MappedFieldType fieldType = new RangeFieldMapper.Builder(FIELD_NAME, RangeType.DATE).fieldType();
        fieldType.setName(FIELD_NAME);
        final DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("_name").field(FIELD_NAME);
        if (configure != null) {
            configure.accept(aggregationBuilder);
        }
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(DateHistogramAggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalDateHistogram> verify,
                          MappedFieldType fieldType) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        DateRangeHistogramAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher,
            fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalDateHistogram) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    private static ZonedDateTime asZDT(String dateTime) {
        return Instant.ofEpochMilli(asLong(dateTime)).atZone(ZoneOffset.UTC);
    }
}
