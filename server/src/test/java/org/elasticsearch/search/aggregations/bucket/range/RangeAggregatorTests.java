/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LongScriptFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.StringScriptFieldTermQuery;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RangeAggregatorTests extends AggregatorTestCase {

    private static final String NUMBER_FIELD_NAME = "number";
    private static final String DATE_FIELD_NAME = "date";

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(0, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertFalse(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7), new IntPoint(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(List.of(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 2), new IntPoint(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(List.of(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 3), new IntPoint(NUMBER_FIELD_NAME, 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 7), new IntPoint(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 2), new IntPoint(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 3), new IntPoint(NUMBER_FIELD_NAME, 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testUnboundedRanges() throws IOException {
        testCase(
            new RangeAggregationBuilder("name").field(NUMBER_FIELD_NAME).addUnboundedTo(5).addUnboundedFrom(5),
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(
                    List.of(
                        new NumericDocValuesField(NUMBER_FIELD_NAME, Integer.MIN_VALUE),
                        new IntPoint(NUMBER_FIELD_NAME, Integer.MIN_VALUE)
                    )
                );
                iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 7), new IntPoint(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 2), new IntPoint(NUMBER_FIELD_NAME, 2)));
                iw.addDocument(List.of(new NumericDocValuesField(NUMBER_FIELD_NAME, 3), new IntPoint(NUMBER_FIELD_NAME, 3)));
                iw.addDocument(
                    List.of(
                        new NumericDocValuesField(NUMBER_FIELD_NAME, Integer.MAX_VALUE),
                        new IntPoint(NUMBER_FIELD_NAME, Integer.MAX_VALUE)
                    )
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
            new NumberFieldMapper.NumberFieldType(
                NUMBER_FIELD_NAME,
                NumberFieldMapper.NumberType.INTEGER,
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                null,
                Collections.emptyMap(),
                null,
                false,
                null
            )
        );
    }

    public void testDateFieldMillisecondResolution() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(
            DATE_FIELD_NAME,
            randomBoolean(),
            randomBoolean(),
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );

        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(DATE_FIELD_NAME)
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1), new LongPoint(DATE_FIELD_NAME, milli1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2), new LongPoint(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testDateFieldNanosecondResolution() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME, true, false, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, DateFieldMapper.Resolution.NANOSECONDS, null, null, Collections.emptyMap());

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(DATE_FIELD_NAME)
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, TimeUnit.MILLISECONDS.toNanos(milli1))));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, TimeUnit.MILLISECONDS.toNanos(milli2))));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void  testMissingDateWithDateNanosField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME, true, false, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, DateFieldMapper.Resolution.NANOSECONDS, null, null, Collections.emptyMap());

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, TimeUnit.MILLISECONDS.toNanos(milli1))));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, TimeUnit.MILLISECONDS.toNanos(milli2))));
            // Missing will apply to this document
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testNotFitIntoDouble() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            NUMBER_FIELD_NAME,
            NumberType.LONG,
            true,
            false,
            true,
            false,
            null,
            Collections.emptyMap(),
            null,
            false,
            null
        );

        long start = 2L << 54; // Double stores 53 bits of mantissa, so we aggregate a bunch of bigger values

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(NUMBER_FIELD_NAME)
            .addRange(start, start + 50)
            .addRange(start + 50, start + 100)
            .addUnboundedFrom(start + 100);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (long l = start; l < start + 150; l++) {
                iw.addDocument(List.of(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, l), new LongPoint(NUMBER_FIELD_NAME, l)));
            }
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertThat(ranges, hasSize(3));
            // If we had a native `double` range aggregator we'd get 50, 50, 50
            assertThat(ranges.stream().mapToLong(InternalRange.Bucket::getDocCount).toArray(), equalTo(new long[] {44, 48, 58}));
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void  testMissingDateWithNumberField() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(NUMBER_FIELD_NAME)
            .addRange(-2d, 5d)
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
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field("does_not_exist")
            .addRange(-2d, 5d)
            .missing(0L);

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
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field("does_not_exist")
            .addRange(-2d, 5d)
            .missing("2020-02-13T10:11:12");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnsupportedType() {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field("not_a_number")
            .addRange(-2d, 5d);

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, range -> fail("Should have thrown exception"), fieldType));
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [range]", e.getMessage());
    }

    public void testBadMissingField() {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(NUMBER_FIELD_NAME)
            .addRange(-2d, 5d)
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
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field("does_not_exist")
            .addRange(-2d, 5d)
            .missing("bogus");

        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testSubAggCollectsFromSingleBucketIfOneRange() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test")
            .field(NUMBER_FIELD_NAME)
            .addRange(0d, 10d)
            .subAggregation(aggCardinality("c"));

        simpleTestCase(aggregationBuilder, new MatchAllDocsQuery(), range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            InternalAggCardinality pc = ranges.get(0).getAggregations().get("c");
            assertThat(pc.cardinality(), equalTo(CardinalityUpperBound.ONE));
        });
    }

    public void testSubAggCollectsFromManyBucketsIfManyRanges() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test")
            .field(NUMBER_FIELD_NAME)
            .addRange(0d, 10d)
            .addRange(10d, 100d)
            .subAggregation(aggCardinality("c"));

        simpleTestCase(aggregationBuilder, new MatchAllDocsQuery(), range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            InternalAggCardinality pc = ranges.get(0).getAggregations().get("c");
            assertThat(pc.cardinality().map(i -> i), equalTo(2));
            pc = ranges.get(1).getAggregations().get("c");
            assertThat(pc.cardinality().map(i -> i), equalTo(2));
        });
    }

    public void testOverlappingRanges() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(NUMBER_FIELD_NAME);
        aggregationBuilder.addRange(0d, 5d);
        aggregationBuilder.addRange(10d, 20d);
        aggregationBuilder.addRange(0d, 20d);
        aggregationBuilder.missing(100);            // Set a missing value to force the "normal" range collection instead of filter-based
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 11)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 3)));
        }, result -> {
            InternalRange<?, ?> range = (InternalRange<?, ?>) result;
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertThat(ranges, hasSize(3));
            assertThat(ranges.get(0).getFrom(), equalTo(0d));
            assertThat(ranges.get(0).getTo(), equalTo(5d));
            assertThat(ranges.get(0).getDocCount(), equalTo(2L));
            assertThat(ranges.get(1).getFrom(), equalTo(00d));
            assertThat(ranges.get(1).getTo(), equalTo(20d));
            assertThat(ranges.get(1).getDocCount(), equalTo(4L));
            assertThat(ranges.get(2).getFrom(), equalTo(10d));
            assertThat(ranges.get(2).getTo(), equalTo(20d));
            assertThat(ranges.get(2).getDocCount(), equalTo(1L));
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER));
    }

    /**
     * If the top level query is a runtime field we use the standard aggregator
     * because it's marginally faster. You'd expect it to be a *ton* faster but
     * usually the ranges drive the iteration and they are still fairly fast.
     * But the union operation overhead that comes with combining the range with
     * the top level query tends to slow us down more than the standard aggregator.
     */
    public void testRuntimeFieldTopLevelQueryNotOptimized() throws IOException {
        long totalDocs = (long) RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS * 4;
        SearchLookup lookup = new SearchLookup(s -> null, (ft, l) -> null);
        StringFieldScript.LeafFactory scriptFactory = ctx -> new StringFieldScript("dummy", Map.of(), lookup, ctx) {
            @Override
            public void execute() {
                emit("cat");
            }
        };
        Query query = new StringScriptFieldTermQuery(new Script("dummy"), scriptFactory, "dummy", "cat", false);
        debugTestCase(new RangeAggregationBuilder("r").field(NUMBER_FIELD_NAME).addRange(0, 1).addRange(1, 2).addRange(2, 3), query, iw -> {
            for (int d = 0; d < totalDocs; d++) {
                iw.addDocument(List.of(new IntPoint(NUMBER_FIELD_NAME, 0), new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 0)));
            }
        }, (InternalRange<?, ?> r, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
            assertThat(
                r.getBuckets().stream().map(InternalRange.Bucket::getKey).collect(toList()),
                equalTo(List.of("0.0-1.0", "1.0-2.0", "2.0-3.0"))
            );
            assertThat(
                r.getBuckets().stream().map(InternalRange.Bucket::getDocCount).collect(toList()),
                equalTo(List.of(totalDocs, 0L, 0L))
            );
            assertThat(impl, equalTo(RangeAggregator.NoOverlap.class));
            assertMap(debug, matchesMap().entry("r", matchesMap().entry("ranges", 3).entry("average_docs_per_range", closeTo(6667, 1))));
        }, new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER));
    }

    /**
     * If the field we're getting the range of is a runtime field it'd be super
     * slow to run a bunch of range queries on it so we disable the optimization.
     */
    public void testRuntimeFieldRangesNotOptimized() throws IOException {
        long totalDocs = (long) RangeAggregator.DOCS_PER_RANGE_TO_USE_FILTERS * 4;
        LongFieldScript.Factory scriptFactory = (fieldName, params, l) -> ctx -> new LongFieldScript(fieldName, Map.of(), l, ctx) {
            @Override
            public void execute() {
                emit((long) getDoc().get(NUMBER_FIELD_NAME).get(0));
            }
        };
        MappedFieldType dummyFt = new LongScriptFieldType("dummy", scriptFactory, new Script("test"), Map.of());
        MappedFieldType numberFt = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);
        debugTestCase(
            new RangeAggregationBuilder("r").field("dummy").addRange(0, 1).addRange(1, 2).addRange(2, 3),
            new MatchAllDocsQuery(),
            iw -> {
                for (int d = 0; d < totalDocs; d++) {
                    iw.addDocument(List.of(new IntPoint(NUMBER_FIELD_NAME, 0), new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 0)));
                }
            },
            (InternalRange<?, ?> r, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(
                    r.getBuckets().stream().map(InternalRange.Bucket::getKey).collect(toList()),
                    equalTo(List.of("0.0-1.0", "1.0-2.0", "2.0-3.0"))
                );
                assertThat(
                    r.getBuckets().stream().map(InternalRange.Bucket::getDocCount).collect(toList()),
                    equalTo(List.of(totalDocs, 0L, 0L))
                );

                assertThat(impl, equalTo(RangeAggregator.NoOverlap.class));
                assertMap(
                    debug,
                    matchesMap().entry("r", matchesMap().entry("ranges", 3).entry("average_docs_per_range", closeTo(6667, 1)))
                );
            },
            dummyFt,
            numberFt
        );
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            NUMBER_FIELD_NAME,
            NumberFieldMapper.NumberType.INTEGER,
            randomBoolean(),
            randomBoolean(),
            true,
            false,
            null,
            Collections.emptyMap(),
            null,
            false,
            null
        );
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(NUMBER_FIELD_NAME);
        aggregationBuilder.addRange(0d, 5d);
        aggregationBuilder.addRange(10d, 20d);
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void simpleTestCase(
        RangeAggregationBuilder aggregationBuilder,
        Query query,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 3)));
        }, verify, fieldType);
    }

    private void testCase(RangeAggregationBuilder aggregationBuilder,
                          Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>>> verify,
                          MappedFieldType fieldType) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange<?, ?>> agg = searchAndReduce(indexSearcher,
                    query, aggregationBuilder, fieldType);
                verify.accept(agg);

            }
        }
    }
}
