/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.shard.ShardMetrics;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.codec.CodecTests.getLucene90StoredFieldsFormatMode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class CodecIntegrationTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(ShardMetrics.CODEC_METRICS_ENABLED.getKey(), true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    /** With the node setting on, indexing, merging and fetching all run through {@link MetricingCodec} and a clean run counts nothing. */
    public void testCodecMetricsWiredThroughEngine() {
        createIndex("index1");
        int docs = between(2, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("index1").setSource("field", randomAlphaOfLength(10)).setRefreshPolicy(IMMEDIATE).get();
        }
        indicesAdmin().prepareForceMerge("index1").setMaxNumSegments(1).get();
        assertHitCount(client().prepareSearch("index1").setSize(docs).addFetchField("field"), docs);
        TestTelemetryPlugin telemetry = getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .get();
        assertThat(telemetry.getLongCounterMeasurement(CodecMetrics.CODEC_FAILURE_TOTAL), empty());
    }

    public void testCanConfigureLegacySettings() {
        createIndex("index1", Settings.builder().put("index.codec", "legacy_default").build());
        var codec = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "index1")
            .execute()
            .actionGet()
            .getSetting("index1", "index.codec");
        assertThat(codec, equalTo("legacy_default"));

        createIndex("index2", Settings.builder().put("index.codec", "legacy_best_compression").build());
        codec = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "index2")
            .execute()
            .actionGet()
            .getSetting("index2", "index.codec");
        assertThat(codec, equalTo("legacy_best_compression"));
    }

    public void testDefaultCodecLogsdb() {
        var indexService = createIndex("index1", Settings.builder().put("index.mode", "logsdb").build());
        var storedFieldsFormat = (Zstd814StoredFieldsFormat) writeStoredFieldsFormat(indexService);
        assertThat(storedFieldsFormat.getMode(), equalTo(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION));
    }

    public void testDefaultCodec() throws Exception {
        var indexService = createIndex("index1");
        var storedFieldsFormat = (Lucene90StoredFieldsFormat) writeStoredFieldsFormat(indexService);
        var mode = getLucene90StoredFieldsFormatMode(storedFieldsFormat);
        assertThat(mode, equalTo(Lucene90StoredFieldsFormat.Mode.BEST_SPEED));
    }

    /**
     * The node setting is on here, so the engine writes through a {@link MetricingCodec} whose stored fields format is a
     * {@link TSDBStoredFieldsFormat} over an {@link ElasticsearchStoredFieldsFormat}; unwrap all three to reach the concrete Lucene format.
     */
    private static StoredFieldsFormat writeStoredFieldsFormat(IndexService indexService) {
        Codec codec = ((MetricingCodec) indexService.getShard(0).getEngineOrNull().config().getCodec()).delegate();
        var codecFormat = (ElasticsearchStoredFieldsFormat) ((TSDBStoredFieldsFormat) codec.storedFieldsFormat()).delegate();
        return codecFormat.writeFormat();
    }
}
