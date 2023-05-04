/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.TimeUnit;

public class SearchIdleStatsTests extends ESSingleNodeTestCase {

    public void testSearchIdleStats() throws InterruptedException {
        // GIVEN
        String index = "test";
        createIndex(index);
        ensureGreen(index);
        final IndicesStatsResponse indicesStatsBefore = client().admin().indices().prepareStats(index).get();
        assertFalse(indicesStatsBefore.getTotal().getSearch().isSearchIdle());

        // WHEN
        int searchIdleAfter = randomIntBetween(100, 200);
        assertTrue(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(
                        Settings.builder().put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), (searchIdleAfter + "ms")).build(),
                        index
                    )
                )
                .actionGet()
                .isAcknowledged()
        );
        waitUntil(() -> client().admin().indices().prepareStats(index).get().getTotal().getSearch().isSearchIdle(), 1, TimeUnit.SECONDS);

        // THEN
        final IndicesStatsResponse indicesStatsAfter = client().admin().indices().prepareStats(index).get();
        assertTrue(indicesStatsAfter.getTotal().getSearch().isSearchIdle());
        assertTrue(indicesStatsAfter.getTotal().getSearch().getSearchIdleTime() >= searchIdleAfter);
    }
}
