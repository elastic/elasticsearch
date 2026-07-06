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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.ES95CodecClusterSettingProvider;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.SuppressCodecs("*")
public class TSDBES95CodecDefaultIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testEs95EnabledByDefault() throws Exception {
        final String dataStreamName = randomDataStreamName();
        putTsdbTemplate(dataStreamName, Settings.EMPTY);
        triggerBackingIndexCreation(dataStreamName);

        final String backingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
        final String settingKey = IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey();

        final var withoutDefaults = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex))
            .actionGet();
        assertThat(withoutDefaults.getIndexToSettings().get(backingIndex).get(settingKey), nullValue());

        final var withDefaults = indicesAdmin().getSettings(
            new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex).includeDefaults(true)
        ).actionGet();
        assertThat(withDefaults.getIndexToDefaultSettings().get(backingIndex).get(settingKey), equalTo("true"));
    }

    public void testExplicitOptOutPreserved() throws Exception {
        final String dataStreamName = randomDataStreamName();
        putTsdbTemplate(
            dataStreamName,
            Settings.builder().put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), false).build()
        );
        triggerBackingIndexCreation(dataStreamName);

        final String backingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
        final Settings settings = indexSettingsFor(backingIndex);

        assertThat(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.get(settings), equalTo(false));
    }

    public void testClusterSettingDisablesCodecForNewIndices() throws Exception {
        final String clusterKey = ES95CodecClusterSettingProvider.TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(clusterKey, false));
        try {
            final String dataStreamName = randomDataStreamName();
            putTsdbTemplate(dataStreamName, Settings.EMPTY);
            triggerBackingIndexCreation(dataStreamName);

            final String backingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
            final Settings settings = indexSettingsFor(backingIndex);

            assertThat(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.get(settings), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(clusterKey));
        }
    }

    public void testClusterSettingOverridesExplicitOptIn() throws Exception {
        final String clusterKey = ES95CodecClusterSettingProvider.TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(clusterKey, false));
        try {
            final String dataStreamName = randomDataStreamName();
            putTsdbTemplate(
                dataStreamName,
                Settings.builder().put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true).build()
            );
            triggerBackingIndexCreation(dataStreamName);

            final String backingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
            final Settings settings = indexSettingsFor(backingIndex);

            assertThat(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.get(settings), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(clusterKey));
        }
    }

    public void testClusterSettingDisablesCodecForStandaloneIndex() throws Exception {
        final String clusterKey = ES95CodecClusterSettingProvider.TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(clusterKey, false));
        try {
            final String indexName = "tsdb-standalone-" + randomIdentifier();
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("hostname"))
                .put("index.time_series.start_time", "2024-01-01T00:00:00Z")
                .put("index.time_series.end_time", "2025-01-01T00:00:00Z")
                .build();
            final String mapping = """
                {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "hostname":   { "type": "keyword", "time_series_dimension": true },
                    "metric":     { "type": "long" }
                  }
                }
                """;
            assertAcked(indicesAdmin().prepareCreate(indexName).setSettings(settings).setMapping(mapping));

            final Settings indexSettings = indexSettingsFor(indexName);
            assertThat(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.get(indexSettings), equalTo(false));
        } finally {
            updateClusterSettings(Settings.builder().putNull(clusterKey));
        }
    }

    public void testSettingIsFinal() throws Exception {
        final String dataStreamName = randomDataStreamName();
        putTsdbTemplate(dataStreamName, Settings.EMPTY);
        triggerBackingIndexCreation(dataStreamName);

        final String backingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
        final Settings update = Settings.builder().put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), false).build();

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings(backingIndex).setSettings(update).get()
        );
        assertThat(exception.getMessage(), containsString(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey()));
        assertThat(exception.getMessage(), containsString("cannot be modified on an index once it is created"));
    }

    private static String randomDataStreamName() {
        return "metrics-" + randomIdentifier();
    }

    private static void putTsdbTemplate(String dataStreamName, Settings extraSettings) throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("hostname"))
            .put(extraSettings)
            .build();
        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "hostname":   { "type": "keyword", "time_series_dimension": true },
                  "metric":     { "type": "long" }
                }
              }
            }
            """;
        final TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            "template-" + dataStreamName.toLowerCase(Locale.ROOT)
        ).indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(new Template(settings, new CompressedXContent(mapping), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
    }

    private static void triggerBackingIndexCreation(String dataStreamName) {
        final String source = "{\"@timestamp\":\"" + Instant.now() + "\",\"hostname\":\"vm-01\",\"metric\":1}";
        client().prepareIndex(dataStreamName).setSource(source, XContentType.JSON).setOpType(DocWriteRequest.OpType.CREATE).get();
    }

    private static Settings indexSettingsFor(String indexName) {
        final var response = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName).get();
        final Settings settings = response.getIndexToSettings().get(indexName);
        assertThat("settings for " + indexName, settings, notNullValue());
        return settings;
    }
}
