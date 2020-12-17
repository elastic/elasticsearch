/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class RollupActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private String index;
    private String rollupIndex;
    private long startTime;
    private int docCount;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Rollup.class, AnalyticsPlugin.class, AggregateMetricMapperPlugin.class);
    }

    @Before
    public void setup() {
        index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        rollupIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(10, 1000);

        client().admin().indices().prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).build())
            .setMapping(
                "date_1", "type=date",
                "numeric_1", "type=double",
                "numeric_2", "type=float",
                "categorical_1", "type=keyword").get();
    }

    public void testTermsGrouping() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    public void testHistogramGrouping() throws IOException {
        long interval = randomLongBetween(1, 1000);
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDoubleBetween(0.0, 10000.0, true))
            .field("numeric_2", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, new HistogramGroupConfig(interval, "numeric_1"), null),
            Collections.singletonList(new MetricConfig("numeric_2", Collections.singletonList("max"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    public void testMaxMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    public void testMinMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("min"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    public void testValueCountMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("value_count"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    public void testAvgMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            // use integers to ensure that avg is comparable between rollup and original
            .field("numeric_1", randomInt())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("avg"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
    }

    private DateHistogramGroupConfig randomDateHistogramGroupConfig() {
        final String timezone = randomBoolean() ? randomDateTimeZone().toString() : null;
        final DateHistogramInterval interval;
        if (randomBoolean()) {
            interval = new DateHistogramInterval(randomTimeValue(2, 1000, new String[]{"d", "h", "ms", "s", "m"}));
            return new DateHistogramGroupConfig.FixedInterval("date_1", interval, null, timezone);
        } else {
            interval = new DateHistogramInterval(randomTimeValue(1,1, "m", "h", "d", "w"));
            return new DateHistogramGroupConfig.CalendarInterval("date_1", interval, null, timezone);
        }
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        final long maxNumBuckets = 10;
        final long endTime = startTime + maxNumBuckets * interval.estimateMillis();
        return DATE_FORMATTER.formatMillis(randomLongBetween(startTime, endTime));
    }

    private void bulkIndex(SourceSupplier sourceSupplier) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(index);
            XContentBuilder source = sourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
        assertHitCount(client().prepareSearch(index).setSize(0).get(), docCount);
    }

    private void rollup(RollupActionConfig config) {
        RollupAction.Response rollupResponse = client().execute(RollupAction.INSTANCE,
            new RollupAction.Request(index, rollupIndex, config)).actionGet();
        assertTrue(rollupResponse.isCreated());
    }

    private void assertRollupIndex(RollupActionConfig config) {
        // TODO(talevy): assert mapping
        // TODO(talevy): assert settings

        final CompositeAggregationBuilder aggregation = buildCompositeAggs("resp", config);
        long numBuckets = 0;
        InternalComposite origResp = client().prepareSearch(index).addAggregation(aggregation).get().getAggregations().get("resp");
        InternalComposite rollupResp = client().prepareSearch(rollupIndex).addAggregation(aggregation).get().getAggregations().get("resp");
        while (origResp.afterKey() != null) {
            numBuckets += origResp.getBuckets().size();
            assertThat(origResp, equalTo(rollupResp));
            aggregation.aggregateAfter(origResp.afterKey());
            origResp = client().prepareSearch(index).addAggregation(aggregation).get().getAggregations().get("resp");
            rollupResp = client().prepareSearch(rollupIndex).addAggregation(aggregation).get().getAggregations().get("resp");
        }
        assertThat(origResp, equalTo(rollupResp));

        SearchResponse resp = client().prepareSearch(rollupIndex).setTrackTotalHits(true).get();
        assertThat(resp.getHits().getTotalHits().value, equalTo(numBuckets));
    }

    private CompositeAggregationBuilder buildCompositeAggs(String name, RollupActionConfig config) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();

        DateHistogramGroupConfig dateHistoConfig = config.getGroupConfig().getDateHistogram();
        DateHistogramValuesSourceBuilder dateHisto = new DateHistogramValuesSourceBuilder(dateHistoConfig.getField());
        dateHisto.field(dateHistoConfig.getField());
        if (dateHistoConfig.getTimeZone() != null) {
            dateHisto.timeZone(ZoneId.of(dateHistoConfig.getTimeZone()));
        }
        if (dateHistoConfig instanceof DateHistogramGroupConfig.FixedInterval) {
            dateHisto.fixedInterval(dateHistoConfig.getInterval());
        } else if (dateHistoConfig instanceof DateHistogramGroupConfig.CalendarInterval) {
            dateHisto.calendarInterval(dateHistoConfig.getInterval());
        } else {
            dateHisto.interval(dateHistoConfig.getInterval().estimateMillis());
            assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
        }
        sources.add(dateHisto);

        if (config.getGroupConfig().getHistogram() != null) {
            HistogramGroupConfig histoConfig = config.getGroupConfig().getHistogram();
            for (String field : histoConfig.getFields()) {
                HistogramValuesSourceBuilder source = new HistogramValuesSourceBuilder(field)
                    .field(field)
                    .interval(histoConfig.getInterval());
                sources.add(source);
            }
        }

        if (config.getGroupConfig().getTerms() != null) {
            TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
            for (String field : termsConfig.getFields()) {
                TermsValuesSourceBuilder source = new TermsValuesSourceBuilder(field).field(field);
                sources.add(source);
            }
        }

        final CompositeAggregationBuilder composite = new CompositeAggregationBuilder(name, sources).size(100);
        if (config.getMetricsConfig() != null) {
            for (MetricConfig metricConfig : config.getMetricsConfig()) {
                for (String metricName : metricConfig.getMetrics()) {
                    switch (metricName) {
                        case "min":
                            composite.subAggregation(new MinAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "max":
                            composite.subAggregation(new MaxAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "sum":
                            composite.subAggregation(new SumAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "value_count":
                            composite.subAggregation(new ValueCountAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "avg":
                            composite.subAggregation(new AvgAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported metric type [" + metricName + "]");
                    }
                }
            }
        }
        return composite;
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }
}

