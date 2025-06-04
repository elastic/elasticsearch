/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class DateRangeAggregatorTests extends AggregatorTestCase {

    private static final String NUMBER_FIELD_NAME = "number";
    private static final String DATE_FIELD_NAME = "date";

    private static final Instant T1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant();
    private static final Instant T2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant();

    /**
     * Dates used by scripting tests.
     */
    private static final List<List<Instant>> DATE_FIELD_VALUES = List.of(
        List.of(
            ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            ZonedDateTime.of(2012, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()
        ),
        List.of(
            ZonedDateTime.of(2012, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            ZonedDateTime.of(2012, 3, 3, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()
        ),
        List.of(
            ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            ZonedDateTime.of(2012, 3, 16, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()
        ),
        List.of(
            ZonedDateTime.of(2012, 3, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
            ZonedDateTime.of(2012, 4, 3, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()
        )
    );

    private static final String VALUE_SCRIPT_NAME = "value_script";
    private static final String FIELD_SCRIPT_NAME = "field_script";

    public void testBooleanFieldDeprecated() throws IOException {
        final String fieldName = "bogusBoolean";
        testCase(new DateRangeAggregationBuilder("name").field(fieldName).addRange("false", "true"), new MatchAllDocsQuery(), iw -> {
            Document d = new Document();
            d.add(new SortedNumericDocValuesField(fieldName, 0));
            iw.addDocument(d);
        }, a -> {}, new BooleanFieldMapper.BooleanFieldType(fieldName));
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

    public void testMissingDateStringWithDateField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME);

        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(DATE_FIELD_NAME)
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
        testCase(iw -> {
            iw.addDocument(
                List.of(new NumericDocValuesField(DATE_FIELD_NAME, Long.MIN_VALUE), new LongPoint(DATE_FIELD_NAME, Long.MIN_VALUE))
            );
            iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 7), new LongPoint(DATE_FIELD_NAME, 7)));
            iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 2), new LongPoint(DATE_FIELD_NAME, 2)));
            iw.addDocument(List.of(new NumericDocValuesField(DATE_FIELD_NAME, 3), new LongPoint(DATE_FIELD_NAME, 3)));
            iw.addDocument(
                List.of(new NumericDocValuesField(DATE_FIELD_NAME, Long.MAX_VALUE), new LongPoint(DATE_FIELD_NAME, Long.MAX_VALUE))
            );
        }, result -> {
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
            new AggTestConfig(
                new RangeAggregationBuilder("name").field(DATE_FIELD_NAME).addUnboundedTo(5).addUnboundedFrom(5),
                new DateFieldMapper.DateFieldType(
                    DATE_FIELD_NAME,
                    randomBoolean(),
                    randomBoolean(),
                    true,
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                    Resolution.MILLISECONDS,
                    null,
                    null,
                    emptyMap()
                )
            )
        );
    }

    public void testNumberFieldDateRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testNumberFieldNumberRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange(0, 5);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

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

    public void testMissingDateStringWithNumberField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14")
            .missing("1979-01-01T00:00:00");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithoutMissing() throws IOException {
        List<Consumer<DateRangeAggregationBuilder>> rangeTypes = List.of(
            builder -> builder.addRange("2015-01-01", "2015-12-31"),
            builder -> builder.addRange(
                ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                ZonedDateTime.of(2015, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)
            )
        );

        for (Consumer<DateRangeAggregationBuilder> rangeType : rangeTypes) {
            final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist");
            rangeType.accept(aggregationBuilder);

            testCase(iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, (InternalDateRange range) -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(0, ranges.get(0).getDocCount());
                assertFalse(AggregationInspectionHelper.hasValue(range));
            }, new AggTestConfig(aggregationBuilder));
        }
    }

    public void testUnmappedWithMissingNumber() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing(1447438575000L); // 2015-11-13 6:16:15

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

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
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing("2015-11-13T10:11:12");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

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
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("not_a_number")
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, range -> fail("Should have thrown exception"), fieldType)
        );
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [date_range]", e.getMessage());
    }

    public void testBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(ElasticsearchParseException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testValueScriptSingleValuedField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(DATE_FIELD_NAME)
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap()));

        testCase(iw -> {
            for (List<Instant> values : DATE_FIELD_VALUES) {
                iw.addDocument(List.of(new SortedNumericDocValuesField(DATE_FIELD_NAME, values.get(0).toEpochMilli())));
            }
        }, (InternalDateRange range) -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertThat(ranges, hasSize(3));

            {
                Range.Bucket bucket = ranges.get(0);
                assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getFrom(), nullValue());
                assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getFromAsString(), nullValue());
                assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }

            {
                Range.Bucket bucket = ranges.get(1);
                assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }

            {
                Range.Bucket bucket = ranges.get(2);
                assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getTo(), nullValue());
                assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getToAsString(), nullValue());
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }, new AggTestConfig(aggregationBuilder, new DateFieldMapper.DateFieldType(DATE_FIELD_NAME)));
    }

    public void testValueScriptMultiValuedField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(DATE_FIELD_NAME)
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap()));

        testCase(iw -> {
            for (List<Instant> values : DATE_FIELD_VALUES) {
                iw.addDocument(
                    values.stream().map(value -> new SortedNumericDocValuesField(DATE_FIELD_NAME, value.toEpochMilli())).toList()
                );
            }
        }, (InternalDateRange range) -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertThat(ranges, hasSize(3));

            {
                Range.Bucket bucket = ranges.get(0);
                assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getFrom(), nullValue());
                assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getFromAsString(), nullValue());
                assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }

            {
                Range.Bucket bucket = ranges.get(1);
                assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getDocCount(), equalTo(2L));
            }

            {
                Range.Bucket bucket = ranges.get(2);
                assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                assertThat(bucket.getTo(), nullValue());
                assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                assertThat(bucket.getToAsString(), nullValue());
                assertThat(bucket.getDocCount(), equalTo(3L));
            }
        }, new AggTestConfig(aggregationBuilder, new DateFieldMapper.DateFieldType(DATE_FIELD_NAME)));
    }

    private void testBothResolutions(
        Query query,
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.Resolution, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify
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

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify,
        DateFieldMapper.Resolution resolution
    ) throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(
            DATE_FIELD_NAME,
            true,
            false,
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            resolution,
            null,
            null,
            emptyMap()
        );
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(
        DateRangeAggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify,
        MappedFieldType fieldType
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>> agg = searchAndReduce(
                    indexReader,
                    new AggTestConfig(aggregationBuilder, fieldType).withQuery(query)
                );
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

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.DATE);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new DateRangeAggregationBuilder("_name").field(fieldName).addRange("2015-01-01", "2015-12-31");
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = Map.of(VALUE_SCRIPT_NAME, vars -> {
            Number value = (Number) vars.get("_value");
            return Instant.ofEpochMilli(value.longValue()).atZone(ZoneOffset.UTC).plusMonths(1).toInstant().toEpochMilli();
        }, FIELD_SCRIPT_NAME, vars -> {
            String fieldName = (String) vars.get("field");
            LeafDocLookup lookup = (LeafDocLookup) vars.get("doc");
            return lookup.get(fieldName).stream().map(value -> ((ZonedDateTime) value).plusMonths(1).toInstant().toEpochMilli()).toList();
        });
        final MockScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, emptyMap());
        final Map<String, ScriptEngine> engines = Map.of(engine.getType(), engine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 0);
    }
}
