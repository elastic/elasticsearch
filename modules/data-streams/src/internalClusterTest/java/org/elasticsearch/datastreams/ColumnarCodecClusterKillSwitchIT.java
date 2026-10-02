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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.ColumnarCodecClusterSettingProvider;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

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
        final String index = createColumnarIndex("columnar-on-" + randomIdentifier());
        assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(index)), equalTo(true));
    }

    public void testSwitchOffDisablesNewColumnarIndex() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        updateClusterSettings(Settings.builder().put(CLUSTER_KEY, false));
        try {
            final String index = createColumnarIndex("columnar-off-" + randomIdentifier());
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(index)), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(CLUSTER_KEY));
        }
    }

    public void testExistingColumnarIndexUnaffectedWhenSwitchFlippedOff() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final String existing = createColumnarIndex("columnar-existing-" + randomIdentifier());
        assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(existing)), equalTo(true));

        updateClusterSettings(Settings.builder().put(CLUSTER_KEY, false));
        try {
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(existing)), equalTo(true));

            final String fresh = createColumnarIndex("columnar-fresh-" + randomIdentifier());
            assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(fresh)), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(CLUSTER_KEY));
        }
    }

    public void testSwitchOffDoesNotStickyThroughDataStreamSettingsUpdate() throws Exception {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final String dsName = "ds-columnar-" + randomIdentifier();
        putColumnarDataStreamTemplate(dsName, mode);
        triggerDataStreamCreation(dsName);

        updateClusterSettings(Settings.builder().put(CLUSTER_KEY, false));
        try {
            client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT
                ).indices(dsName)
            ).actionGet();
        } finally {
            updateClusterSettings(Settings.builder().putNull(CLUSTER_KEY));
        }

        assertAcked(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dsName, null)).actionGet());
        final String newBackingIndex = getDataStreamBackingIndexNames(dsName).get(1);
        assertThat(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.get(indexSettingsFor(newBackingIndex)), equalTo(true));
    }

    private String createColumnarIndex(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(INDEX_KEY, true)
            .build();
        assertAcked(indicesAdmin().prepareCreate(indexName).setSettings(settings).setMapping(MAPPING));
        client().prepareIndex(indexName).setSource("{\"kwd\":\"a\"}", XContentType.JSON).get();
        indicesAdmin().prepareRefresh(indexName).get();
        return indexName;
    }

    private void triggerDataStreamCreation(String dsName) {
        client().prepareIndex(dsName)
            .setSource("{\"@timestamp\":\"" + Instant.now() + "\",\"kwd\":\"a\"}", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .get();
    }

    private static void putColumnarDataStreamTemplate(String dsName, IndexMode mode) throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).put(INDEX_KEY, true).build();
        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "kwd": { "type": "keyword" }
                }
              }
            }
            """;
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("template-" + dsName.toLowerCase(Locale.ROOT)).indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsName + "*"))
                        .template(new Template(settings, new CompressedXContent(mapping), null))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                        .build()
                )
            ).actionGet()
        );
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
