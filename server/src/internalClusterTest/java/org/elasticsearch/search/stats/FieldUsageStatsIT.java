/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class FieldUsageStatsIT extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.aggs.rewrite_to_filter_by_filter", false).build();
    }

    public void testFieldUsageStats() {
        int numShards = 2;
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)));

        for (int i = 1; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(
                "field", "value", "field2", "value2", "date_field", "2015/09/0" + i).get();
        }
        client().admin().indices().prepareRefresh("test").get();

        FieldUsageStats stats = client().admin().indices().prepareStats("test").clear().setFieldUsage(true).get()
            .getIndex("test").getTotal().getFieldUsageStats();

        assertFalse(stats.hasField("field"));
        assertFalse(stats.hasField("field.keyword"));
        assertFalse(stats.hasField("field2"));
        assertFalse(stats.hasField("date_field"));

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .addAggregation(AggregationBuilders.filter("agg2", QueryBuilders.spanTermQuery("field2", "value2")))
            .setSize(100)
            .get();
        assertHitCount(searchResponse, 9);
        assertAllSuccessful(searchResponse);

        stats = client().admin().indices().prepareStats("test").clear().setFieldUsage(true).get()
            .getIndex("test").getTotal().getFieldUsageStats();
        logger.info("Stats after first query: {}", stats);

        assertTrue(stats.hasField("_id"));
        assertEquals(Set.of(FieldUsageStats.UsageContext.STORED_FIELDS), stats.get("_id").keySet());
        assertTrue(stats.hasField("_source"));
        assertEquals(Set.of(FieldUsageStats.UsageContext.STORED_FIELDS), stats.get("_source").keySet());

        assertTrue(stats.hasField("field"));
        // we sort by _score
        assertEquals(Set.of(FieldUsageStats.UsageContext.TERMS, FieldUsageStats.UsageContext.FREQS, FieldUsageStats.UsageContext.NORMS),
            stats.get("field").keySet());
        assertEquals(1L * numShards, stats.get("field").getTerms());

        assertTrue(stats.hasField("field2"));
        // positions because of span query
        assertEquals(Set.of(FieldUsageStats.UsageContext.TERMS, FieldUsageStats.UsageContext.FREQS, FieldUsageStats.UsageContext.POSITIONS),
            stats.get("field2").keySet());
        assertEquals(1L * numShards, stats.get("field2").getTerms());

        assertTrue(stats.hasField("field.keyword"));
        // terms agg does not use search as we've set search.aggs.rewrite_to_filter_by_filter to false
        assertEquals(Set.of(FieldUsageStats.UsageContext.DOC_VALUES), stats.get("field.keyword").keySet());
        assertEquals(1L * numShards, stats.get("field.keyword").getDocValues());

        client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .setSize(100)
            .get();

        stats = client().admin().indices().prepareStats("test").clear().setFieldUsage(true).get()
            .getIndex("test").getTotal().getFieldUsageStats();
        logger.info("Stats after second query: {}", stats);

        assertEquals(2L * numShards, stats.get("field").getTerms());
        assertEquals(1L * numShards, stats.get("field2").getTerms());
        assertEquals(2L * numShards, stats.get("field.keyword").getDocValues());

        assertFalse(stats.hasField("date_field"));

        // show that we also track stats in can_match
        assertEquals(2L * numShards, client().admin().indices().prepareStats("test").clear().setSearch(true).get()
            .getIndex("test").getTotal().getSearch().getTotal().getQueryCount());
        client().prepareSearch()
            .setPreFilterShardSize(1)
            .setQuery(QueryBuilders.rangeQuery("date_field").from("2016/01/01"))
            .setSize(100)
            .get();

        stats = client().admin().indices().prepareStats("test").clear().setFieldUsage(true).get()
            .getIndex("test").getTotal().getFieldUsageStats();
        logger.info("Stats after third query: {}", stats);

        assertTrue(stats.hasField("date_field"));
        assertEquals(Set.of(FieldUsageStats.UsageContext.POINTS), stats.get("date_field").keySet());
        // can_match does not enter search stats
        // there is a special case though where we have no hit but we need to get at least one search response in order
        // to produce a valid search result with all the aggs etc., so we hit one of the two shards
        assertEquals((2 * numShards) + 1, client().admin().indices().prepareStats("test").clear().setSearch(true).get()
            .getIndex("test").getTotal().getSearch().getTotal().getQueryCount());
    }
}
