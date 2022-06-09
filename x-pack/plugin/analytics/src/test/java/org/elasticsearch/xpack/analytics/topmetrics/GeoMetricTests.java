/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.timeseries.InternalTimeSeries;
import org.elasticsearch.search.aggregations.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class GeoMetricTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AnalyticsPlugin.class);
    }

    public void testwithMatchAllQuery() throws Exception {
        testWithquery(new MatchAllQueryBuilder());
    }

    public void testWithQueryInDimension() throws Exception {
        testWithquery(new TermsQueryBuilder("asset_id", "asset1", "asset9"));
    }

    public void testWithQueryInMetric() throws Exception {
        testWithquery(new RangeQueryBuilder("location").gt(0L).lt(Long.MAX_VALUE / 2));
    }

    public void testWithQueryInDimensionAndMetric() throws Exception {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(new TermsQueryBuilder("asset_id", "asset1", "asset9"));
        queryBuilder.filter(new RangeQueryBuilder("location").gt(0L).lt(Long.MAX_VALUE / 2));
        testWithquery(queryBuilder);
    }

    private void testWithquery(QueryBuilder queryBuilder) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("asset_id")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .startObject("location")
            .field("type", "long")
            // .field("time_series_metric", "position")
            .endObject()
            .endObject()
            .endObject();

        Settings.Builder s = Settings.builder()
            .put("index.mode", "time_series")
            .put("index.routing_path", "asset_id")
            .put("index.number_of_shards", 1);

        client().admin().indices().prepareCreate("test").setMapping(xcb).setSettings(s).get();
        ensureGreen();

        for (int k = 0; k < 10; k++) {
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < randomIntBetween(1, 20); j++) {
                    xcb = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("asset_id", "asset" + i)
                        .field("@timestamp", (k * 20) + j)
                        .field("location", randomLong())
                        .endObject();
                    client().prepareIndex("test").setSource(xcb).setRefreshPolicy(IMMEDIATE).get();
                }
            }
        }

        if (randomBoolean()) {
            client().admin().indices().forceMerge(new ForceMergeRequest().indices("test").maxNumSegments(1)).get();
        }
        ensureGreen();

        for (int size = 1; size < 10; size++) {
            TimeSeriesAggregationBuilder builder = new TimeSeriesAggregationBuilder("by_time_series", true, size);
            FieldSortBuilder sortBuilder = new FieldSortBuilder("@timestamp").order(SortOrder.DESC);
            MultiValuesSourceFieldConfig.Builder metricField = new MultiValuesSourceFieldConfig.Builder();
            metricField.setFieldName("location");

            TopMetricsAggregationBuilder top = new TopMetricsAggregationBuilder(
                "last_value",
                List.of(sortBuilder),
                1,
                List.of(metricField.build())
            );
            builder.subAggregation(top);

            // query as time series
            SearchResponse searchResponse1 = client().prepareSearch("test").setQuery(queryBuilder).addAggregation(builder).setSize(0).get();

            TermsAggregationBuilder terms = new TermsAggregationBuilder("by_asset_id").size(1000).field("asset_id");
            terms.subAggregation(top);

            // query as standard index
            SearchResponse searchResponse2 = client().prepareSearch("test").setQuery(queryBuilder).addAggregation(terms).setSize(0).get();
            InternalTimeSeries ts1 = searchResponse1.getAggregations().get("by_time_series");
            StringTerms ts2 = searchResponse2.getAggregations().get("by_asset_id");
            assertEquals(ts1.getBuckets().size(), ts2.getBuckets().size());
            for (int i = 0; i < ts1.getBuckets().size(); i++) {
                assertBucket(ts2, ts1.getBuckets().get(i));
            }
        }
    }

    private void assertBucket(StringTerms ts2, InternalTimeSeries.InternalBucket bucket) {
        for (StringTerms.Bucket bucket1 : ts2.getBuckets()) {
            if (bucket1.getKeyAsString().equals(bucket.getKey().get("asset_id"))) {
                InternalTopMetrics itp1 = bucket.getAggregations().get("last_value");
                InternalTopMetrics itp2 = bucket1.getAggregations().get("last_value");
                assertEquals(itp1, itp2);
                return;
            }
        }
        fail("not found");
    }
}
