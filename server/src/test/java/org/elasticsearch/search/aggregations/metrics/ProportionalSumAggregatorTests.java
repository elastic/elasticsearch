/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;

public class ProportionalSumAggregatorTests extends AggregatorTestCase {

    private static final String RANGE_FIELD = "span";
    private static final String VALUE_FIELD = "val";

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), dateHistogramAggregationBuilder("month", true), "val", iw -> {
            iw.addDocument(doc("2010-03-12T01:07:45", "2010-03-12T02:07:45", new NumericDocValuesField("wrong_val", 102)));
            iw.addDocument(doc("2010-04-01T03:43:34", "2010-04-01T04:43:34", new NumericDocValuesField("wrong_val", 103)));
            iw.addDocument(doc("2010-04-27T03:43:34", "2010-04-27T04:43:34", new NumericDocValuesField("wrong_val", 103)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(2));
            assertThat(dh.getBuckets().get(0).getAggregations().asList(), hasSize(1));
            assertThat(dh.getBuckets().get(0).getAggregations().asList().get(0), instanceOf(InternalSum.class));
            assertThat(((InternalSum) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(0.0, 0.000001));

            assertThat(dh.getBuckets().get(1).getAggregations().asList(), hasSize(1));
            assertThat(dh.getBuckets().get(1).getAggregations().asList().get(0), instanceOf(InternalSum.class));
            assertThat(((InternalSum) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(0.0, 0.000001));
        });
    }

    private <T extends AggregationBuilder> void testDocValuesPerMinute(T parentAggregationBuilder) throws IOException {
        testCase(new MatchAllDocsQuery(), parentAggregationBuilder, VALUE_FIELD, iw -> {
            iw.addDocument(doc("2010-03-12T01:07:30", "2010-03-12T01:09:30", new NumericDocValuesField(VALUE_FIELD, 4)));
            iw.addDocument(doc("2010-03-12T01:08:30", "2010-03-12T01:10:30", new NumericDocValuesField(VALUE_FIELD, 40)));
            iw.addDocument(doc("2010-03-12T01:09:30", "2010-03-12T01:11:30", new NumericDocValuesField(VALUE_FIELD, 400)));
        }, dh -> {
            assertThat(dh.getBuckets(), hasSize(5));
            // 07: 1
            assertThat(((InternalSum) dh.getBuckets().get(0).getAggregations().asList().get(0)).value(), closeTo(1.0, 0.000001));
            // 08: 2 + 10
            assertThat(((InternalSum) dh.getBuckets().get(1).getAggregations().asList().get(0)).value(), closeTo(12.0, 0.000001));
            // 09: 1 + 20 + 100
            assertThat(((InternalSum) dh.getBuckets().get(2).getAggregations().asList().get(0)).value(), closeTo(121.0, 0.000001));
            // 10:     10 + 200
            assertThat(((InternalSum) dh.getBuckets().get(3).getAggregations().asList().get(0)).value(), closeTo(210.0, 0.000001));
            // 11:          100
            assertThat(((InternalSum) dh.getBuckets().get(4).getAggregations().asList().get(0)).value(), closeTo(100.0, 0.000001));
        });
    }

    public void testDateHistogramDocValuesPerMinute() throws IOException {
        testDocValuesPerMinute(dateHistogramAggregationBuilder("minute", true));
    }

    public void testHistogramDocValuesPerMinute() throws IOException {
        testDocValuesPerMinute(histogramAggregationBuilder(60000));
    }

    private <P extends AggregationBuilder,
        B extends InternalMultiBucketAggregation.InternalBucket,
        A extends InternalMultiBucketAggregation<A, B>
        > void testCase(
        Query query,
        P parentAggregationBuilder,
        Object field,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<A> verify
    ) throws IOException {
        MappedFieldType rangeFieldType = new RangeFieldMapper.Builder(RANGE_FIELD, RangeType.DATE, true)
            .build(new ContentPath())
            .fieldType();

        MappedFieldType numType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        ProportionalSumAggregationBuilder proportionalSumAggregationBuilder = new ProportionalSumAggregationBuilder("my_prop_sum");
        if (field != null) {
            if (field instanceof Script) {
                proportionalSumAggregationBuilder.script((Script) field);
            } else {
                proportionalSumAggregationBuilder.field((String) field);
            }
        }
        parentAggregationBuilder.subAggregation(proportionalSumAggregationBuilder);
        testCase(parentAggregationBuilder, query, buildIndex, verify, rangeFieldType, numType);
    }

    private DateHistogramAggregationBuilder dateHistogramAggregationBuilder(String interval, boolean isCalendar) {
        DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("my_date");
        aggregationBuilder.field(RANGE_FIELD);
        if (isCalendar) {
            aggregationBuilder.calendarInterval(new DateHistogramInterval(interval));
        } else {
            aggregationBuilder.fixedInterval(new DateHistogramInterval(interval));
        }
        return aggregationBuilder;
    }

    private HistogramAggregationBuilder histogramAggregationBuilder(double interval) {
        HistogramAggregationBuilder aggregationBuilder = new HistogramAggregationBuilder("my_date");
        aggregationBuilder.field(RANGE_FIELD);
        aggregationBuilder.interval(interval);
        return aggregationBuilder;
    }

    private Iterable<IndexableField> doc(String gte, String lt, IndexableField... fields) throws IOException {
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DATE, asLong(gte), asLong(lt), true, false);

        Field field = new BinaryDocValuesField(RANGE_FIELD, RangeType.DATE.encodeRanges(singleton(range)));

        List<IndexableField> indexableFields = new ArrayList<>();
        indexableFields.add(field);
        indexableFields.addAll(Arrays.asList(fields));
        return indexableFields;
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

}
