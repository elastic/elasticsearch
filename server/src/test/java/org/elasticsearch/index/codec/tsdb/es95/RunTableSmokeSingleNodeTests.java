/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class RunTableSmokeSingleNodeTests extends ESSingleNodeTestCase {

    private static final int NUM_SERIES = 8;
    private static final int DOCS_PER_SERIES = 40;
    private static final long BASE_TS = 1704067200000L;

    public void testRunTableDimensionQueriesEndToEnd() throws Exception {
        final String index = "runtable-smoke";
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true)
            .build();

        createIndex(
            index,
            settings,
            "@timestamp",
            "type=date",
            "host",
            "type=keyword,time_series_dimension=true",
            "tags",
            "type=keyword",
            "value",
            "type=long,time_series_metric=gauge"
        );

        for (int s = 0; s < NUM_SERIES; s++) {
            for (int i = 0; i < DOCS_PER_SERIES; i++) {
                final XContentBuilder source = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", BASE_TS + (s * DOCS_PER_SERIES + i) * 1000L)
                    .field("host", "host-" + s)
                    .array("tags", "env-" + (s % 2), "region-" + (s % 3))
                    .field("value", (long) (s * 1000 + i))
                    .endObject();
                prepareIndex(index).setSource(source).get();
            }
        }
        indicesAdmin().prepareRefresh(index).get();

        // Term filter over the single-valued dimension.
        assertResponse(
            client().prepareSearch(index).setQuery(QueryBuilders.termQuery("host", "host-3")).setTrackTotalHits(true),
            response -> assertHitCount(response, DOCS_PER_SERIES)
        );

        // Term filter over the multi-valued dimension: env-0 covers even series (0,2,4,6).
        assertResponse(
            client().prepareSearch(index).setQuery(QueryBuilders.termQuery("tags", "env-0")).setTrackTotalHits(true),
            response -> assertHitCount(response, 4L * DOCS_PER_SERIES)
        );

        // Terms aggregation grouping by the single-valued dimension: one bucket per series.
        assertResponse(
            client().prepareSearch(index)
                .setSize(0)
                .addAggregation(AggregationBuilders.terms("by_host").field("host").size(NUM_SERIES + 4)),
            response -> {
                final Terms terms = response.getAggregations().get("by_host");
                assertEquals(NUM_SERIES, terms.getBuckets().size());
                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    assertEquals(DOCS_PER_SERIES, bucket.getDocCount());
                }
            }
        );

        // Terms aggregation grouping by the multi-valued dimension: region-* buckets partition all docs.
        assertResponse(
            client().prepareSearch(index).setSize(0).addAggregation(AggregationBuilders.terms("by_tag").field("tags").size(16)),
            response -> {
                final StringTerms terms = response.getAggregations().get("by_tag");
                assertNotNull(terms.getBucketByKey("region-0"));
                assertNotNull(terms.getBucketByKey("env-0"));
                assertNotNull(terms.getBucketByKey("env-1"));
            }
        );

        // Range on the metric plus a sort on it: value < 1000 is exactly series 0 (values 0..39).
        assertResponse(
            client().prepareSearch(index)
                .setQuery(QueryBuilders.rangeQuery("value").lt(1000))
                .addSort("value", SortOrder.ASC)
                .setSize(DOCS_PER_SERIES)
                .setTrackTotalHits(true),
            response -> {
                assertHitCount(response, DOCS_PER_SERIES);
                long previous = Long.MIN_VALUE;
                for (final SearchHit hit : response.getHits().getHits()) {
                    final long v = ((Number) hit.getSortValues()[0]).longValue();
                    assertTrue("sort must be ascending", v >= previous);
                    previous = v;
                }
            }
        );

        // Exists over the multi-valued dimension: every doc carries tags.
        assertResponse(
            client().prepareSearch(index).setQuery(QueryBuilders.existsQuery("tags")).setTrackTotalHits(true),
            response -> assertHitCount(response, (long) NUM_SERIES * DOCS_PER_SERIES)
        );

        // Missing over a field no doc carries: zero hits.
        assertResponse(
            client().prepareSearch(index)
                .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("host")))
                .setTrackTotalHits(true),
            response -> assertHitCount(response, 0L)
        );
    }
}
