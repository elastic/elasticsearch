/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
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
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        TermsAggregationBuilder termsAggBuilder = new TermsAggregationBuilder("terms");
        termsAggBuilder.field("categorical_1");
        if (randomBoolean()) {
            termsAggBuilder.order(BucketOrder.aggregation("max_numeric_1", false));
        }
        termsAggBuilder.subAggregation(new MaxAggregationBuilder("max_numeric_1").field("numeric_1"));
        aggBuilder.subAggregation(termsAggBuilder);
        assertAggregation(aggBuilder);
    }

    public void testHistogramGrouping() throws IOException {
        long interval = randomLongBetween(1, 1000);
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDoubleBetween(0.0, 100.0, true))
            .field("numeric_2", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, new HistogramGroupConfig(interval, "numeric_1"), null),
            Collections.singletonList(new MetricConfig("numeric_2", Collections.singletonList("max"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        HistogramAggregationBuilder histoAggBuilder = new HistogramAggregationBuilder("histo");
        histoAggBuilder.field("numeric_1");
        histoAggBuilder.interval(interval);
        histoAggBuilder.subAggregation(new MaxAggregationBuilder("max_numeric_2").field("numeric_2"));
        aggBuilder.subAggregation(histoAggBuilder);
        assertAggregation(aggBuilder);
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
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new MaxAggregationBuilder("max_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
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
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new MinAggregationBuilder("min_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
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
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new ValueCountAggregationBuilder("value_count_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    public void testAvgMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("avg"))));
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new ValueCountAggregationBuilder("avg_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    private DateHistogramAggregationBuilder dateHistogramBuilder(String name, DateHistogramGroupConfig dateHistogramGroupConfig) {
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder(name);
        aggBuilder.field(dateHistogramGroupConfig.getField());
        aggBuilder.timeZone(ZoneId.of(dateHistogramGroupConfig.getTimeZone()));
        if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.FixedInterval) {
            aggBuilder.fixedInterval(dateHistogramGroupConfig.getInterval());
        } else if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.CalendarInterval) {
            aggBuilder.calendarInterval(dateHistogramGroupConfig.getInterval());
        } else {
            aggBuilder.interval(dateHistogramGroupConfig.getInterval().estimateMillis());
            assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
        }
        return aggBuilder;
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
        GetIndexResponse indexResponse = client().admin().indices().prepareGetIndex().setIndices(rollupIndex).get();
        assertTrue(true);
        // TODO(talevy): assert doc count
        // TODO(talevy): assert mapping
        // TODO(talevy): assert settings
    }

    private void assertAggregation(AggregationBuilder builder) {
        Aggregations indexAggregations = client().prepareSearch(index).addAggregation(builder).get().getAggregations();
        Aggregations rollupAggregations = client().prepareSearch(rollupIndex).addAggregation(builder).get().getAggregations();
        assertAggregations(indexAggregations, rollupAggregations);
    }

    private void assertAggregations(Aggregations indexAggregations, Aggregations rollupAggregations) {
        List<Aggregation> indexAggregationsList = indexAggregations.asList();
        List<Aggregation> rollupAggregationsList = rollupAggregations.asList();
        assertThat(rollupAggregationsList.size(), equalTo(indexAggregationsList.size()));
        for (int i = 0; i < rollupAggregationsList.size(); i++) {
            Aggregation indexAggObj = indexAggregationsList.get(i);
            Aggregation rollupAggObj = rollupAggregationsList.get(i);
            if (indexAggObj instanceof MultiBucketsAggregation && rollupAggObj instanceof MultiBucketsAggregation) {
                MultiBucketsAggregation indexAgg = (MultiBucketsAggregation) indexAggObj;
                MultiBucketsAggregation rollupAgg = (MultiBucketsAggregation) rollupAggObj;
                assertThat(rollupAgg.getBuckets().size(), equalTo(indexAgg.getBuckets().size()));
                for (int j = 0; j < rollupAggregationsList.size(); j++) {
                    assertThat(rollupAgg.getBuckets().get(j).getKey(), equalTo(indexAgg.getBuckets().get(j).getKey()));
                    assertThat(rollupAgg.getBuckets().get(j).getDocCount(), equalTo(indexAgg.getBuckets().get(j).getDocCount()));
                    assertAggregations(indexAgg.getBuckets().get(j).getAggregations(), rollupAgg.getBuckets().get(j).getAggregations());
                }
            } else if (indexAggObj instanceof InternalNumericMetricsAggregation.SingleValue
                    && rollupAggObj instanceof InternalNumericMetricsAggregation.SingleValue) {
                InternalNumericMetricsAggregation.SingleValue indexAgg = (InternalNumericMetricsAggregation.SingleValue) indexAggObj;
                InternalNumericMetricsAggregation.SingleValue rollupAgg = (InternalNumericMetricsAggregation.SingleValue) rollupAggObj;
                assertThat(rollupAgg.getValueAsString(), equalTo(indexAgg.getValueAsString()));
            }
            else {
                throw new IllegalArgumentException("unsupported aggregation type [" + indexAggObj.getType() + "]");
            }
        }
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }
}

