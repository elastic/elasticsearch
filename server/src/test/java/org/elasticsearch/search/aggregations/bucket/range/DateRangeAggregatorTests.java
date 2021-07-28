/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DateRangeAggregatorTests extends AggregatorTestCase {

    private static final String NUMBER_FIELD_NAME = "number";
    private static final String DATE_FIELD_NAME = "date";

    private static final Instant T1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant();
    private static final Instant T2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant();

    public void testBooleanFieldDeprecated() throws IOException {
        final String fieldName = "bogusBoolean";
        testCase(
            new DateRangeAggregationBuilder("name").field(fieldName).addRange("false", "true"),
            new MatchAllDocsQuery(),
            iw -> {
                Document d = new Document();
                d.add(new SortedNumericDocValuesField(fieldName, 0));
                iw.addDocument(d);
            },
            a -> {},
            new BooleanFieldMapper.BooleanFieldType(fieldName)
        );
        assertWarnings("Running Range or DateRange aggregations on [boolean] fields is deprecated");
    }

    public void testNoMatchingField() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), (iw, resolution) -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", resolution.convert(Instant.ofEpochMilli(7)))));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", resolution.convert(Instant.ofEpochMilli(2)))));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", resolution.convert(Instant.ofEpochMilli(3)))));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(0, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertFalse(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), (iw, resolution) -> {
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(DATE_FIELD_NAME, resolution.convert(T1)),
                    new LongPoint(DATE_FIELD_NAME, resolution.convert(T1))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(DATE_FIELD_NAME, resolution.convert(T2)),
                    new LongPoint(DATE_FIELD_NAME, resolution.convert(T2))
                )
            );
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), (iw, resolution) -> {
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(DATE_FIELD_NAME, resolution.convert(T1)),
                    new LongPoint(DATE_FIELD_NAME, resolution.convert(T1))
                )
            );
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(DATE_FIELD_NAME, resolution.convert(T2)),
                    new LongPoint(DATE_FIELD_NAME, resolution.convert(T2))
                )
            );
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void  testMissingDateStringWithDateField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME);

        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange("2015-11-13", "2015-11-14");

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, T1.toEpochMilli())));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, T2.toEpochMilli())));
            // Missing will apply to this document
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testUnboundedRanges() throws IOException {
        testCase(
            new RangeAggregationBuilder("name").field(DATE_FIELD_NAME).addUnboundedTo(5).addUnboundedFrom(5),
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(
                    List.of(new NumericDocValuesField(DATE_FIELD_NAME, Long.MIN_VALUE), new LongPoint(DATE_FIELD_NAME, Long.MIN_VALUE))
                );
                iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 7), new LongPoint(DATE_FIELD_NAME, 7)));
                iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 2), new LongPoint(DATE_FIELD_NAME, 2)));
                iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 3), new LongPoint(DATE_FIELD_NAME, 3)));
                iw.addDocument(
                    List.of(new NumericDocValuesField(DATE_FIELD_NAME, Long.MAX_VALUE), new LongPoint(DATE_FIELD_NAME, Long.MAX_VALUE))
                );
            },
            result -> {
                InternalRange<?, ?> range = (InternalRange<?, ?>) result;
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertThat(ranges, hasSize(2));
                assertThat(ranges.get(0).getFrom(), equalTo(Double.NEGATIVE_INFINITY));
                assertThat(ranges.get(0).getTo(), equalTo(5d));
                assertThat(ranges.get(0).getDocCount(), equalTo(3L));
                assertThat(ranges.get(1).getFrom(), equalTo(5d));
                assertThat(ranges.get(1).getTo(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(ranges.get(1).getDocCount(), equalTo(2L));
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            new DateFieldMapper.DateFieldType(
                DATE_FIELD_NAME,
                randomBoolean(),
                randomBoolean(),
                true,
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                Resolution.MILLISECONDS,
                null,
                null,
                Collections.emptyMap()
            )
        );
    }

    public void  testNumberFieldDateRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void  testNumberFieldNumberRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange(0, 5);

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 7), new IntPoint(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 1), new IntPoint(NUMBER_FIELD_NAME, 1)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void  testMissingDateStringWithNumberField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14")
            .missing("1979-01-01T00:00:00");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithMissingNumber() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing(1447438575000L); // 2015-11-13 6:16:15

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testUnmappedWithMissingDate() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing("2015-11-13T10:11:12");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

            testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(2, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }, fieldType);
    }

    public void testKeywordField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("not_a_number")
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, range -> fail("Should have thrown exception"), fieldType));
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [date_range]",
            e.getMessage());
    }

    public void testBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(ElasticsearchParseException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    private void testBothResolutions(
        Query query,
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.Resolution, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify
    ) throws IOException {
        testCase(
            query,
            iw -> buildIndex.accept(iw, DateFieldMapper.Resolution.MILLISECONDS),
            verify,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        testCase(
            query,
            iw -> buildIndex.accept(iw, DateFieldMapper.Resolution.NANOSECONDS),
            verify,
            DateFieldMapper.Resolution.NANOSECONDS
        );
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
                          DateFieldMapper.Resolution resolution) throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME, true, false, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, resolution, null, null, Collections.emptyMap());
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(DateRangeAggregationBuilder aggregationBuilder,
                          Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
                          MappedFieldType fieldType) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> agg = searchAndReduce(indexSearcher,
                    query, aggregationBuilder, fieldType);
                verify.accept(agg);

            }
        }
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }
}
