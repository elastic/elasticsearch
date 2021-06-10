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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.ShardFieldUsageStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class FieldUsageStatsIT extends ESSingleNodeTestCase {

    public void testFieldUsageStats() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        IndexShard indexShard = null;
        for (IndexService indexService : getInstanceFromNode(IndicesService.class)) {
            if (indexService.index().getName().equals("test")) {
                indexShard = indexService.getShard(0);
                break;
            }
        }

        assertNotNull(indexShard);
        int docsTest1 = scaledRandomIntBetween(1, 5);
        for (int i = 0; i < docsTest1; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(
                "field", "value", "field2", "value2").get();
        }
        client().admin().indices().prepareRefresh("test").get();

        final ShardFieldUsageStats stats = indexShard.fieldUsageStats();

        assertFalse(stats.getPerFieldStats().containsKey("field"));
        assertFalse(stats.getPerFieldStats().containsKey("field.keyword"));

        assertFalse(stats.getPerFieldStats().containsKey("field"));
        assertFalse(stats.getPerFieldStats().containsKey("field.keyword"));

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .addAggregation(AggregationBuilders.filter("agg2", QueryBuilders.termQuery("field2", "value2")))
            .setSize(100)
            .get();
        assertHitCount(searchResponse, docsTest1);
        assertAllSuccessful(searchResponse);

        assertFalse(stats.getPerFieldStats().containsKey("field"));
        assertTrue(stats.getPerFieldStats().containsKey("field.keyword"));
        assertEquals(1L, stats.getPerFieldStats().get("field.keyword").getAggregationCount());

        client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .setSize(100)
            .get();

        assertEquals(2L, stats.getPerFieldStats().get("field.keyword").getAggregationCount());
    }
}
