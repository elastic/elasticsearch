/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexSettingProviderTests extends ESSingleNodeTestCase {

    public void testIndexCreation() throws Exception {
        var indexService = createIndex("my-index1");
        assertFalse(indexService.getIndexSettings().getSettings().hasValue("index.refresh_interval"));

        INDEX_SETTING_PROVIDER1_ENABLED.set(true);
        indexService = createIndex("my-index2");
        assertTrue(indexService.getIndexSettings().getSettings().hasValue("index.refresh_interval"));

        INDEX_SETTING_PROVIDER2_ENABLED.set(true);
        var e = expectThrows(IllegalArgumentException.class, () -> createIndex("my-index3"));
        assertEquals(
            "additional index setting [index.refresh_interval] added by [TestIndexSettingsProvider] is already present",
            e.getMessage()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Plugin1.class, Plugin2.class);
    }

    public static class Plugin1 extends Plugin {

        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new TestIndexSettingsProvider("index.refresh_interval", "-1", INDEX_SETTING_PROVIDER1_ENABLED));
        }

    }

    public static class Plugin2 extends Plugin {

        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new TestIndexSettingsProvider("index.refresh_interval", "100s", INDEX_SETTING_PROVIDER2_ENABLED));
        }
    }

    private static final AtomicBoolean INDEX_SETTING_PROVIDER1_ENABLED = new AtomicBoolean(false);
    private static final AtomicBoolean INDEX_SETTING_PROVIDER2_ENABLED = new AtomicBoolean(false);

    static class TestIndexSettingsProvider implements IndexSettingProvider {

        private final String settingName;
        private final String settingValue;
        private final AtomicBoolean enabled;

        TestIndexSettingsProvider(String settingName, String settingValue, AtomicBoolean enabled) {
            this.settingName = settingName;
            this.settingValue = settingValue;
            this.enabled = enabled;
        }

        @Override
        public Settings getAdditionalIndexSettings(
            String indexName,
            String dataStreamName,
            boolean isTimeSeries,
            Metadata metadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings
        ) {
            if (enabled.get()) {
                return Settings.builder().put(settingName, settingValue).build();
            } else {
                return Settings.EMPTY;
            }
        }
    }
}
