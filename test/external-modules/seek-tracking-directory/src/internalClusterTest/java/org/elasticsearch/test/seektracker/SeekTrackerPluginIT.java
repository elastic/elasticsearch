/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

public class SeekTrackerPluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SeekTrackerPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SeekTrackerPlugin.SEEK_TRACKING_ENABLED.getKey(), "true")
            .build();
    }

    public void testSeekTrackerPlugin() throws InterruptedException {

        assertAcked(client().admin().indices().prepareCreate("index"));
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            docs.add(client().prepareIndex("index").setSource("field", "term" + i % 5));
        }
        indexRandom(true, docs);

        client().prepareSearch("index").setQuery(QueryBuilders.termQuery("field", "term2")).get();

        SeekStatsResponse response = client().execute(SeekStatsAction.INSTANCE, new SeekStatsRequest("index")).actionGet();
        List<ShardSeekStats> shardSeekStats = response.getSeekStats().get("index");
        assertThat(shardSeekStats.size(), greaterThan(0));
    }

}
