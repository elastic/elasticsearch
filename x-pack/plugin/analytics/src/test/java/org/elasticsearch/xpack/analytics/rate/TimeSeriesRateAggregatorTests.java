/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TimeSeriesRateAggregatorTests extends AggregatorTestCase {

    private static final int MILLIS_IN_SECOND = 1_000;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AggregationsPlugin(), new AnalyticsPlugin());
    }

    public void testSimple() throws IOException {
        RateAggregationBuilder builder = new RateAggregationBuilder("counter_field").field("counter_field");
        TimeSeriesAggregationBuilder tsBuilder = new TimeSeriesAggregationBuilder("tsid");
        tsBuilder.subAggregation(builder);
        Consumer<InternalTimeSeries> verifier = r -> {
            assertThat(r.getBuckets(), hasSize(2));
            assertThat(
                ((Rate) r.getBucketByKey("{dim=1}").getAggregations().asList().get(0)).getValue(),
                closeTo(59.0 / 3000.0 * MILLIS_IN_SECOND, 0.00001)
            );
            assertThat(
                ((Rate) r.getBucketByKey("{dim=2}").getAggregations().asList().get(0)).getValue(),
                closeTo(206.0 / 4000.0 * MILLIS_IN_SECOND, 0.00001)
            );
        };
        AggTestConfig aggTestConfig = new AggTestConfig(tsBuilder, timeStampField(), counterField("counter_field"))
            .withSplitLeavesIntoSeperateAggregators(false);
        testCase(iw -> {
            iw.addDocuments(docs(1000, "1", 15, 37, 60, /*reset*/ 14));
            iw.addDocuments(docs(1000, "2", 74, 150, /*reset*/ 50, 90, /*reset*/ 40));
        }, verifier, aggTestConfig);
    }

    public void testNestedWithinDateHistogram() throws IOException {
        RateAggregationBuilder builder = new RateAggregationBuilder("counter_field").field("counter_field");
        DateHistogramAggregationBuilder dateBuilder = new DateHistogramAggregationBuilder("date");
        dateBuilder.field("@timestamp");
        dateBuilder.fixedInterval(DateHistogramInterval.seconds(2));
        dateBuilder.subAggregation(builder);
        TimeSeriesAggregationBuilder tsBuilder = new TimeSeriesAggregationBuilder("tsid");
        tsBuilder.subAggregation(dateBuilder);

        Consumer<InternalTimeSeries> verifier = r -> {
            assertThat(r.getBuckets(), hasSize(2));
            assertThat(r.getBucketByKey("{dim=1}"), instanceOf(InternalTimeSeries.InternalBucket.class));
            InternalDateHistogram hb = r.getBucketByKey("{dim=1}").getAggregations().get("date");
            {
                Rate rate = hb.getBuckets().get(1).getAggregations().get("counter_field");
                assertThat(rate.getValue(), closeTo((/* reset: 60 -> 14 */ 60 + 14 - 60) / 1000.0 * MILLIS_IN_SECOND, 0.00001));
            }
            {
                Rate rate = hb.getBuckets().get(0).getAggregations().get("counter_field");
                assertThat(rate.getValue(), closeTo((37 - 15) / 1000.0 * MILLIS_IN_SECOND, 0.00001));
            }
            hb = r.getBucketByKey("{dim=2}").getAggregations().get("date");
            {
                Rate rate = hb.getBuckets().get(0).getAggregations().get("counter_field");
                assertThat(rate.getValue(), closeTo((150 - 74) / 1000.0 * MILLIS_IN_SECOND, 0.00001));
            }
            {
                Rate rate = hb.getBuckets().get(1).getAggregations().get("counter_field");
                assertThat(rate.getValue(), closeTo((/* reset: 90 -> 40 */ 90 + 40 - 90) / 1000.0 * MILLIS_IN_SECOND, 0.00001));
            }
            {
                Rate rate = hb.getBuckets().get(2).getAggregations().get("counter_field");
                assertThat(rate.getValue(), equalTo(Double.NaN));
            }
        };

        AggTestConfig aggTestConfig = new AggTestConfig(tsBuilder, timeStampField(), counterField("counter_field"))
            .withSplitLeavesIntoSeperateAggregators(false);
        testCase(iw -> {
            /*
            Documents
            ---------

            { doc: 1, dim: 1, timestamp: 2000, value: 15 }
            { doc: 2, dim: 1, timestamp: 3000, value: 37 }
            { doc: 3, dim: 1, timestamp: 4000, value: 60 }
            { doc: 4, dim: 1, timestamp: 5000, value: 14 } *** counter reset ***

            { doc: 5, dim: 2, timestamp: 2000, value: 74 }
            { doc: 6, dim: 2, timestamp: 3000, value: 150 }
            { doc: 7, dim: 2, timestamp: 4000, value: 50 } *** counter reset ***
            { doc: 8, dim: 2, timestamp: 5000, value: 90 }
            { doc: 9, dim: 2, timestamp: 6000, value: 40 } *** counter reset ***

            Date histogram (fixed_interval = 2 seconds)
            -------------------------------------------

            dim: 1
            * bucket 0: { doc: 1, doc: 2 } -> rate: (37 - 15) / 1000
            * bucket 1: { doc: 3, doc: 4 } -> rate: (60 + 14 - 60) / 1000

            dim: 2
            * bucket 0: { doc: 1, doc: 2 } -> rate: (150 - 74) / 1000
            * bucket 1: { doc: 3, doc: 4 } -> rate: (90 + 40 - 90) / 1000
            * bucket 2: { doc: 5 } -> rate: NaN
             */
            iw.addDocuments(docs(2000, "1", 15, 37, 60, /*reset*/ 14));
            iw.addDocuments(docs(2000, "2", 74, 150, /*reset*/ 50, 90, /*reset*/ 40));
        }, verifier, aggTestConfig);
    }

    private List<Document> docs(long startTimestamp, String dim, long... values) throws IOException {

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            documents.add(doc(startTimestamp + (i * 1000L), tsid(dim), values[i]));
        }
        return documents;
    }

    private static BytesReference tsid(String dim) throws IOException {
        TimeSeriesIdFieldMapper.TimeSeriesIdBuilder idBuilder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(null);
        idBuilder.addString("dim", dim);
        return idBuilder.build();
    }

    private Document doc(long timestamp, BytesReference tsid, long counterValue) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("@timestamp", timestamp));
        doc.add(new SortedDocValuesField("_tsid", tsid.toBytesRef()));
        doc.add(new NumericDocValuesField("counter_field", counterValue));
        return doc;
    }

    private MappedFieldType counterField(String name) {
        return new NumberFieldMapper.NumberFieldType(
            name,
            NumberFieldMapper.NumberType.LONG,
            true,
            false,
            true,
            false,
            null,
            Collections.emptyMap(),
            null,
            false,
            TimeSeriesParams.MetricType.COUNTER,
            IndexMode.TIME_SERIES
        );
    }

    private DateFieldMapper.DateFieldType timeStampField() {
        return new DateFieldMapper.DateFieldType(
            "@timestamp",
            true,
            false,
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }

}
