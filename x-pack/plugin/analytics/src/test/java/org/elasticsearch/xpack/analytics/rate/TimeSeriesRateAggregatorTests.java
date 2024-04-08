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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
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
        AggTestConfig aggTestConfig = new AggTestConfig(tsBuilder, timeStampField(), counterField("counter_field"), dimensionField("dim"));
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
                assertThat(rate.getValue(), closeTo((60 - 37 + 14) / 2000.0 * MILLIS_IN_SECOND, 0.00001));
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
                assertThat(rate.getValue(), closeTo(90 / 2000.0 * MILLIS_IN_SECOND, 0.00001));
            }
        };

        AggTestConfig aggTestConfig = new AggTestConfig(tsBuilder, timeStampField(), counterField("counter_field"), dimensionField("dim"))
            .withSplitLeavesIntoSeperateAggregators(false);
        testCase(iw -> {
            iw.addDocuments(docs(2000, "1", 15, 37, 60, /*reset*/ 14));
            iw.addDocuments(docs(2000, "2", 74, 150, /*reset*/ 50, 90, /*reset*/ 40));
        }, verifier, aggTestConfig);
    }

    public void testNestedWithinAutoDateHistogram() throws IOException {
        RateAggregationBuilder builder = new RateAggregationBuilder("counter_field").field("counter_field");
        AutoDateHistogramAggregationBuilder dateBuilder = new AutoDateHistogramAggregationBuilder("date");
        dateBuilder.field("@timestamp");
        dateBuilder.subAggregation(builder);
        TimeSeriesAggregationBuilder tsBuilder = new TimeSeriesAggregationBuilder("tsid");
        tsBuilder.subAggregation(dateBuilder);

        Consumer<InternalTimeSeries> verifier = r -> {
            assertThat(r.getBuckets(), hasSize(2));
            assertThat(r.getBucketByKey("{dim=1}"), instanceOf(InternalTimeSeries.InternalBucket.class));
            InternalDateHistogram hb = r.getBucketByKey("{dim=1}").getAggregations().get("date");
            {
                Rate rate = hb.getBuckets().get(1).getAggregations().get("counter_field");
                assertThat(rate.getValue(), closeTo((60 - 37 + 14) / 2000.0 * MILLIS_IN_SECOND, 0.00001));
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
                assertThat(rate.getValue(), closeTo(90 / 2000.0 * MILLIS_IN_SECOND, 0.00001));
            }
        };

        AggTestConfig aggTestConfig = new AggTestConfig(tsBuilder, timeStampField(), counterField("counter_field"))
            .withSplitLeavesIntoSeperateAggregators(false);
        expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            iw.addDocuments(docs(2000, "1", 15, 37, 60, /*reset*/ 14));
            iw.addDocuments(docs(2000, "2", 74, 150, /*reset*/ 50, 90, /*reset*/ 40));
        }, verifier, aggTestConfig));
    }

    private List<Document> docs(long startTimestamp, String dim, long... values) throws IOException {

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            documents.add(doc(startTimestamp + (i * 1000L), tsid(dim), values[i], dim));
        }
        return documents;
    }

    private static BytesReference tsid(String dim) throws IOException {
        TimeSeriesIdFieldMapper.TimeSeriesIdBuilder idBuilder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(null);
        idBuilder.addString("dim", dim);
        return idBuilder.buildTsidHash();
    }

    private Document doc(long timestamp, BytesReference tsid, long counterValue, String dim) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("@timestamp", timestamp));
        doc.add(new SortedDocValuesField("_tsid", tsid.toBytesRef()));
        doc.add(new NumericDocValuesField("counter_field", counterValue));
        doc.add(new SortedDocValuesField("dim", new BytesRef(dim)));
        return doc;
    }

    private MappedFieldType dimensionField(String name) {
        return new KeywordFieldMapper.Builder(name, IndexVersion.current()).dimension(true)
            .docValues(true)
            .build(MapperBuilderContext.root(true, true))
            .fieldType();
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
