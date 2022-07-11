/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedField;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class DateHistogramAggregatorTestCase extends AggregatorTestCase {
    /**
     * A date that is always "aggregable" because it has doc values but may or
     * may not have a search index. If it doesn't then we can't use our fancy
     * date rounding mechanism that needs to know the minimum and maximum dates
     * it is going to round because it ready *that* out of the search index.
     */
    protected static final String AGGREGABLE_DATE = "aggregable_date";

    protected final <R extends InternalAggregation> void asSubAggTestCase(AggregationBuilder builder, Consumer<R> verify)
        throws IOException {
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex = (iw, dft) -> {
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new Field("k1", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new Field("k2", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedNumericDocValuesField("n", 1)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-03-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new Field("k1", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new Field("k2", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedNumericDocValuesField("n", 2)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2021-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new Field("k1", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new Field("k2", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedNumericDocValuesField("n", 3)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2021-03-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new Field("k1", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedSetDocValuesField("k2", new BytesRef("b")),
                    new Field("k2", new BytesRef("b"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedNumericDocValuesField("n", 4)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("b")),
                    new Field("k1", new BytesRef("b"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedSetDocValuesField("k2", new BytesRef("b")),
                    new Field("k2", new BytesRef("b"), KeywordFieldMapper.Defaults.FIELD_TYPE),
                    new SortedNumericDocValuesField("n", 5)
                )
            );
        };
        asSubAggTestCase(builder, buildIndex, verify);
    }

    protected final <R extends InternalAggregation> void asSubAggTestCase(
        AggregationBuilder builder,
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex,
        Consumer<R> verify
    ) throws IOException {
        MappedField k1f = new MappedField("k1", new KeywordFieldMapper.KeywordFieldType());
        MappedField k2f = new MappedField("k2", new KeywordFieldMapper.KeywordFieldType());
        MappedField nf = new MappedField("n", new NumberFieldMapper.NumberFieldType(NumberType.LONG));
        MappedField df = aggregableDateField(false, randomBoolean());
        testCase(builder, new MatchAllDocsQuery(), iw -> buildIndex.accept(iw, (DateFieldMapper.DateFieldType) df.type()),
            verify, k1f, k2f, nf, df);
    }

    protected final MappedField aggregableDateField(boolean useNanosecondResolution, boolean isSearchable) {
        return aggregableDateField(useNanosecondResolution, isSearchable, DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
    }

    protected final MappedField aggregableDateField(
        boolean useNanosecondResolution,
        boolean isSearchable,
        DateFormatter formatter
    ) {
        return new MappedField(AGGREGABLE_DATE, new DateFieldMapper.DateFieldType(
            isSearchable,
            randomBoolean(),
            true,
            formatter,
            useNanosecondResolution ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        ));
    }
}
