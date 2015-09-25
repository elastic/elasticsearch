/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.shards;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.shards.ShardsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

public class ShardsIT extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, ShardsCollector.NAME)
                .build();
    }

    @Test
    public void testShards() throws Exception {
        logger.debug("--> creating some indices so that shards collector reports data");
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            client().prepareIndex("test-" + i, "foo").setRefresh(true).setSource("field1", "value1").get();
        }

        awaitMarvelDocsCount(greaterThan(0L), ShardsCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ShardsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ShardsCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = ShardsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> shards successfully collected");
    }
}
