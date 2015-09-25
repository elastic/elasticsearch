/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

public class NodeStatsIT extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, NodeStatsCollector.NAME)
                .build();
    }

    @Test
    public void testNodeStats() throws Exception {
        logger.debug("--> creating some indices for future node stats");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "foo").setSource("value", randomInt()).get();
        }

        awaitMarvelDocsCount(greaterThan(0L), NodeStatsCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", NodeStatsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(NodeStatsCollector.TYPE).get();

        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = NodeStatsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> node stats successfully collected");
    }
}
