/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.ColumnarCodecClusterSettingProvider;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressCodecs("*")
public class ColumnarCodecClusterKillSwitchIT extends ESIntegTestCase {

    private static final String CLUSTER_KEY = ColumnarCodecClusterSettingProvider.COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING.getKey();
    private static final String INDEX_KEY = IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey();

    private static final String MAPPING = """
        {
          "properties": {
            "kwd": { "type": "keyword" }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testSwitchOnByDefaultKeepsColumnarOptIn() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final String index = createColumnarIndex("columnar-on-" + randomIdentifier(), true);
        assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(index)), equalTo(true));
    }

    public void testSwitchOffDisablesNewColumnarIndex() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        updateClusterSettings(Settings.builder().put(CLUSTER_KEY, false));
        try {
            final String index = createColumnarIndex("columnar-off-" + randomIdentifier(), true);
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(index)), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(CLUSTER_KEY));
        }
    }

    public void testExistingColumnarIndexUnaffectedWhenSwitchFlippedOff() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final String existing = createColumnarIndex("columnar-existing-" + randomIdentifier(), true);
        assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(existing)), equalTo(true));

        updateClusterSettings(Settings.builder().put(CLUSTER_KEY, false));
        try {
            // The already created index keeps its baked per-index opt-in: the setting is final and the provider only
            // runs at creation, so flipping the switch off does not change it.
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(existing)), equalTo(true));

            // A newly created index picks up the switch and does not adopt the codec.
            final String fresh = createColumnarIndex("columnar-fresh-" + randomIdentifier(), true);
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(fresh)), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(CLUSTER_KEY));
        }
    }

    private String createColumnarIndex(String indexName, boolean columnarOptIn) {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(INDEX_KEY, columnarOptIn)
            .build();
        assertAcked(indicesAdmin().prepareCreate(indexName).setSettings(settings).setMapping(MAPPING));
        return indexName;
    }

    private Settings indexSettingsFor(String indexName) {
        final Settings settings = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get()
            .getIndexToSettings()
            .get(indexName);
        assertThat("settings for " + indexName, settings, notNullValue());
        return settings;
    }
}
