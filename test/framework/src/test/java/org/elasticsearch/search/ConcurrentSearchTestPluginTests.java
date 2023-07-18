/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrentSearchTestPluginTests extends ESSingleNodeTestCase {

    private static boolean pluginEnabled = false;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        if (randomBoolean()) {
            pluginEnabled = false;
            return List.of();
        } else {
            pluginEnabled = true;
            return List.of(ConcurrentSearchTestPlugin.class);
        }
    }

    public void testPlugin() {
        int minDocsPerSlice = randomIntBetween(1, 100);
        Map<String, ?> settings = Map.of(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), minDocsPerSlice);
        if (pluginEnabled) {
            ClusterUpdateSettingsResponse response = clusterAdmin().prepareUpdateSettings()
                .setTransientSettings(settings)
                .execute()
                .actionGet();
            assertTrue(response.isAcknowledged());
            assertThat(
                response.getTransientSettings().getAsInt(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 50_000),
                Matchers.equalTo(minDocsPerSlice)
            );
            Map<String, ?> nullSetting = new HashMap<>();
            nullSetting.put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), null);
            response = clusterAdmin().prepareUpdateSettings().setTransientSettings(nullSetting).execute().actionGet();
            assertTrue(response.isAcknowledged());
            assertThat(
                SearchService.MINIMUM_DOCS_PER_SLICE.get(
                    response.getTransientSettings().getAsSettings(SearchService.MINIMUM_DOCS_PER_SLICE.getKey())
                ),
                Matchers.equalTo(50_000)
            );
        } else {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                clusterAdmin().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
            });
            assertThat(
                ex.getMessage(),
                Matchers.containsString("transient setting [" + SearchService.MINIMUM_DOCS_PER_SLICE.getKey() + "], not recognized")
            );
        }
    }
}
