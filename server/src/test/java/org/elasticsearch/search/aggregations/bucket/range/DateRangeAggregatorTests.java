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

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsNull.nullValue;

public class DateRangeAggregatorTests extends AggregatorTestCase {

    private static final String NUMBER_FIELD_NAME = "number";
    private static final String DATE_FIELD_NAME = "date";

    private static final long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
    private static final long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

    private static final List<List<ZonedDateTime>> DATE_FIELD_VALUES = List.of(
        List.of(
            ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2012, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC)
        ),
        List.of(
            ZonedDateTime.of(2012, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2012, 3, 3, 0, 0, 0, 0, ZoneOffset.UTC)
        ),
        List.of(
            ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2012, 3, 16, 0, 0, 0, 0, ZoneOffset.UTC)
        ),
        List.of(
            ZonedDateTime.of(2012, 3, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2012, 4, 3, 0, 0, 0, 0, ZoneOffset.UTC)
        )
    );

    private static final String VALUE_SCRIPT_NAME = "value_script";
    private static final String FIELD_SCRIPT_NAME = "field_script";

    public void testNoMatchingField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 7)));
                iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 2)));
                iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 3)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals(0, ranges.get(0).getDocCount());
                assertEquals(0, ranges.get(1).getDocCount());
                assertFalse(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
                iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals(0, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testMatchesNumericDocValues() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(DATE_FIELD_NAME, milli1)));
                iw.addDocument(singleton(new NumericDocValuesField(DATE_FIELD_NAME, milli2)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals(0, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testMissingDateStringWithDateField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange("2015-11-13", "2015-11-14");

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
                iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
                // Missing will apply to this document
                iw.addDocument(emptySet());
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(2, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testNumberFieldNumberRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange(0, 5);

        numberFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(1, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testUnmapped() throws IOException {
        final List<Consumer<DateRangeAggregationBuilder>> rangeTypes = List.of(
            builder -> builder.addRange("2015-01-01", "2015-12-31"),
            builder -> builder.addRange(
                ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                ZonedDateTime.of(2015, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)
            )
        );

        for (Consumer<DateRangeAggregationBuilder> rangeType : rangeTypes) {
            final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
                .field("does_not_exist");
            rangeType.accept(aggregationBuilder);

            numberFieldTestCase(
                aggregationBuilder,
                new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
                },
                range -> {
                    List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                    assertEquals(1, ranges.size());
                    assertEquals(0, ranges.get(0).getDocCount());
                    assertFalse(AggregationInspectionHelper.hasValue(range));
                }
            );
        }
    }

    public void testUnmappedWithMissingNumber() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing(1447438575000L); // 2015-11-13 6:16:15

        numberFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(2, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testUnmappedWithMissingDate() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing("2015-11-13T10:11:12");

        numberFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(2, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            }
        );
    }

    public void testBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(NUMBER_FIELD_NAME)
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        expectThrows(
            NumberFormatException.class,
            () -> numberFieldTestCase(
                aggregationBuilder,
                new MatchAllDocsQuery(),
                iw -> {
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
                },
                range -> fail("Should have thrown exception")
            )
        );
    }

    public void testUnmappedWithBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field("does_not_exist")
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        expectThrows(
            ElasticsearchParseException.class,
            () -> numberFieldTestCase(
                aggregationBuilder,
                new MatchAllDocsQuery(),
                    iw -> {
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                    iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
                },
                range -> fail("Should have thrown exception")
            )
        );
    }

    public void testPartiallyUnmapped() throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        fieldType.setName(NUMBER_FIELD_NAME);

        final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(fieldType.name())
            .addRange(0, 5);

        try (Directory mappedDirectory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            try (RandomIndexWriter mappedWriter = new RandomIndexWriter(random(), mappedDirectory)) {
                mappedWriter.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
                mappedWriter.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
            }

            new RandomIndexWriter(random(), unmappedDirectory).close();

            try (IndexReader mappedReader = DirectoryReader.open(mappedDirectory);
                 IndexReader unmappedReader = DirectoryReader.open(unmappedDirectory);
                 MultiReader multiReader = new MultiReader(mappedReader, unmappedReader)) {

                final IndexSearcher searcher = newSearcher(multiReader, true, true);

                final InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> result =
                    searchAndReduce(searcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                final List<? extends InternalRange.Bucket> ranges = result.getBuckets();
                assertEquals(1, ranges.size());
                assertEquals(1, ranges.get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testValueScriptSingleValuedField() throws IOException {
        final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(DATE_FIELD_NAME)
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap()));

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                for (List<ZonedDateTime> values : DATE_FIELD_VALUES) {
                    iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, values.get(0).toInstant().toEpochMilli())));
                }
            },
            range -> {
                final List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertThat(ranges, hasSize(3));

                {
                    final Range.Bucket bucket = ranges.get(0);
                    assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), nullValue());
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), nullValue());
                    assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(1);
                    assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(2);
                    assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), nullValue());
                    assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), nullValue());
                    assertThat(bucket.getDocCount(), equalTo(2L));
                }
            }
        );
    }

    public void testValueScriptMultiValuedField() throws IOException {
        final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .field(DATE_FIELD_NAME)
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap()));

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                for (List<ZonedDateTime> values : DATE_FIELD_VALUES) {
                    final Set<IndexableField> document = values.stream()
                        .map(value -> new SortedNumericDocValuesField(DATE_FIELD_NAME, value.toInstant().toEpochMilli()))
                        .collect(toSet());
                    iw.addDocument(document);
                }
            },
            range -> {
                final List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertThat(ranges, hasSize(3));

                {
                    final Range.Bucket bucket = ranges.get(0);
                    assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), nullValue());
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), nullValue());
                    assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(1);
                    assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(2L));
                }

                {
                    final Range.Bucket bucket = ranges.get(2);
                    assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), nullValue());
                    assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), nullValue());
                    assertThat(bucket.getDocCount(), equalTo(3L));
                }
            }
        );
    }

    public void testFieldScriptSingleValuedField() throws IOException {
        final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", DATE_FIELD_NAME)));

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                for (List<ZonedDateTime> values : DATE_FIELD_VALUES) {
                    iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, values.get(0).toInstant().toEpochMilli())));
                }
            },
            range -> {
                final List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertThat(ranges, hasSize(3));

                {
                    final Range.Bucket bucket = ranges.get(0);
                    assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), nullValue());
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), nullValue());
                    assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(1);
                    assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(2);
                    assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), nullValue());
                    assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), nullValue());
                    assertThat(bucket.getDocCount(), equalTo(2L));
                }
            }
        );
    }

    public void testFieldScriptMultiValuedField() throws IOException {
        final DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range")
            .addUnboundedTo("2012-02-15")
            .addRange("2012-02-15", "2012-03-15")
            .addUnboundedFrom("2012-03-15")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", DATE_FIELD_NAME)));

        dateFieldTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                for (List<ZonedDateTime> values : DATE_FIELD_VALUES) {
                    final Set<IndexableField> document = values.stream()
                        .map(value -> new SortedNumericDocValuesField(DATE_FIELD_NAME, value.toInstant().toEpochMilli()))
                        .collect(toSet());
                    iw.addDocument(document);
                }
            },
            range -> {
                final List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertThat(ranges, hasSize(3));

                {
                    final Range.Bucket bucket = ranges.get(0);
                    assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), nullValue());
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), nullValue());
                    assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }

                {
                    final Range.Bucket bucket = ranges.get(1);
                    assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 2, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getDocCount(), equalTo(2L));
                }

                {
                    final Range.Bucket bucket = ranges.get(2);
                    assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
                    assertThat(bucket.getFrom(), equalTo(ZonedDateTime.of(2012, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)));
                    assertThat(bucket.getTo(), nullValue());
                    assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
                    assertThat(bucket.getToAsString(), nullValue());
                    assertThat(bucket.getDocCount(), equalTo(3L));
                }
            }
        );
    }

    private void dateFieldTestCase(DateRangeAggregationBuilder aggregationBuilder,
                                   Query query,
                                   CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                                   Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify)
                                       throws IOException {
        dateFieldTestCaseWithResolution(aggregationBuilder, query, buildIndex, verify, DateFieldMapper.Resolution.MILLISECONDS);
        dateFieldTestCaseWithResolution(aggregationBuilder, query, buildIndex, verify, DateFieldMapper.Resolution.NANOSECONDS);
    }

    private void dateFieldTestCaseWithResolution(DateRangeAggregationBuilder aggregationBuilder,
                                                 Query query,
                                                 CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                                                 Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
                                                 DateFieldMapper.Resolution resolution) throws IOException {
        final DateFieldMapper.Builder builder = new DateFieldMapper.Builder(DATE_FIELD_NAME)
            .withResolution(resolution);
        final DateFieldMapper.DateFieldType fieldType = builder.fieldType();
        fieldType.setHasDocValues(true);
        fieldType.setName(DATE_FIELD_NAME);
        testCase(aggregationBuilder, query, buildIndex, verify, singleton(fieldType));
    }

    private void numberFieldTestCase(DateRangeAggregationBuilder aggregationBuilder,
                                     Query query,
                                     CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                                     Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify)
                                         throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        fieldType.setName(NUMBER_FIELD_NAME);
        testCase(aggregationBuilder, query, buildIndex, verify, singleton(fieldType));
    }

    private void testCase(DateRangeAggregationBuilder aggregationBuilder,
                          Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
                          Collection<MappedFieldType> fieldTypes) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                final MappedFieldType[] fieldTypeArray = fieldTypes.toArray(new MappedFieldType[0]);
                final InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> agg = searchAndReduce(
                    indexSearcher, query, aggregationBuilder, fieldTypeArray);
                verify.accept(agg);

            }
        }
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return singletonList(CoreValuesSourceType.NUMERIC);
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        final List<String> unsupported = new ArrayList<>(singleton(BooleanFieldMapper.CONTENT_TYPE));
        unsupported.addAll(Arrays.stream(NumberType.values())
            .map(numberType -> new NumberFieldMapper.NumberFieldType(numberType).typeName())
            .collect(toList()));
        return unsupported;
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new DateRangeAggregationBuilder("_name")
            .field(fieldName)
            .addRange("2015-01-01", "2015-12-31");
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = Map.of(
            VALUE_SCRIPT_NAME, vars -> new DateTime(((Number) vars.get("_value")).longValue(), DateTimeZone.UTC).plusMonths(1).getMillis(),
            FIELD_SCRIPT_NAME, vars -> {
                final String fieldName = (String) vars.get("field");
                final LeafDocLookup lookup = (LeafDocLookup) vars.get("doc");
                return lookup.get(fieldName).stream()
                    .map(value -> ((JodaCompatibleZonedDateTime) value).plusMonths(1))
                    .collect(toList());
            }
        );
        final MockScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, emptyMap());
        final Map<String, ScriptEngine> engines = singletonMap(engine.getType(), engine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
