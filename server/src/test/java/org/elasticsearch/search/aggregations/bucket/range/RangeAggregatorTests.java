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
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
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
            null
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

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/57651")
    public void testDateFieldNanosecondResolution() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME, true, false, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, DateFieldMapper.Resolution.NANOSECONDS, null, Collections.emptyMap());

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(DATE_FIELD_NAME)
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/57651")
    public void  testMissingDateWithDateField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME, true, false, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, DateFieldMapper.Resolution.NANOSECONDS, null, Collections.emptyMap());

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range")
            .field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
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

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            NUMBER_FIELD_NAME,
            NumberFieldMapper.NumberType.INTEGER,
            randomBoolean(),
            randomBoolean(),
            true,
            false,
            null,
            null
        );
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(NUMBER_FIELD_NAME);
        aggregationBuilder.addRange(0d, 5d);
        aggregationBuilder.addRange(10d, 20d);
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void simpleTestCase(RangeAggregationBuilder aggregationBuilder,
                          Query query,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify) throws IOException {
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
}
