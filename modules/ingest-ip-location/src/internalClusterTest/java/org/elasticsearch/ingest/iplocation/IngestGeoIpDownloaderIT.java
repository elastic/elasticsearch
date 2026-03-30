/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.AbstractGeoIpIT;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.GeoIpTaskState;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Internal cluster tests for ingest pipeline behavior with geoip processors.
 * These tests were moved from ip-location because they fundamentally depend on
 * the geoip processor type, which is registered by {@link IngestIpLocationPlugin}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, maxNumDataNodes = 1)
public class IngestGeoIpDownloaderIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ReindexPlugin.class,
            IngestGeoIpPlugin.class,
            IngestGeoIpSettingsPlugin.class,
            IngestIpLocationPlugin.class,
            NonGeoProcessorsPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        return settings.build();
    }

    @After
    public void disableDownloader() {
        updateClusterSettings(
            Settings.builder()
                .putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey())
        );
    }

    public void testGeoIpDatabasesDownloadNoGeoipProcessors() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String pipelineId = randomAlphaOfLength(10);
        putGeoIpPipeline(pipelineId);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
            assertNotNull(task);
            assertNotNull(task.getState());
        });
        awaitAllNodesDownloadedDatabases();
        putNonGeoipPipeline(pipelineId);
        assertNotNull(getTask().getState());
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
        });
    }

    public void testDoNotDownloadDatabaseOnPipelineCreation() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String pipelineId = randomIdentifier();

        deleteDatabasesInConfigDirectory();

        putGeoIpPipeline("_id", false);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> assertNotNull(getTask()));

        putGeoIpPipeline(pipelineId, false);
        assertNull(getTask().getState());

        String indexIdentifier = randomIdentifier();
        assertAcked(indicesAdmin().prepareCreate(indexIdentifier).get());
        assertNull(getTask().getState());

        Setting<String> pipelineSetting = randomFrom(IndexSettings.FINAL_PIPELINE, IndexSettings.DEFAULT_PIPELINE);
        Settings indexSettings = Settings.builder().put(pipelineSetting.getKey(), pipelineId).build();
        assertAcked(indicesAdmin().prepareUpdateSettings(indexIdentifier).setSettings(indexSettings).get());
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
        }, 2, TimeUnit.MINUTES);
        awaitAllNodesDownloadedDatabases();

        assertAcked(indicesAdmin().prepareDelete(indexIdentifier).get());
    }

    private void putGeoIpPipeline(String pipelineId) throws IOException {
        putGeoIpPipeline(pipelineId, true);
    }

    private void putGeoIpPipeline(String pipelineId, boolean downloadDatabaseOnPipelineCreation) throws IOException {
        putJsonPipeline(pipelineId, ((builder, params) -> {
            builder.startArray("processors");
            {
                builder.startObject();
                {
                    builder.startObject(NonGeoProcessorsPlugin.NON_GEO_PROCESSOR_TYPE);
                    {
                        builder.field("randomField", randomAlphaOfLength(20));
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.startObject();
                {
                    builder.startObject("geoip");
                    {
                        builder.field("field", "ip");
                        builder.field("target_field", "ip-city");
                        builder.field("database_file", "GeoLite2-City.mmdb");
                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.startObject("geoip");
                    {
                        builder.field("field", "ip");
                        builder.field("target_field", "ip-country");
                        builder.field("database_file", "GeoLite2-Country.mmdb");
                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.startObject("geoip");
                    {
                        builder.field("field", "ip");
                        builder.field("target_field", "ip-asn");
                        builder.field("database_file", "GeoLite2-ASN.mmdb");
                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.startObject("geoip");
                    {
                        builder.field("field", "ip");
                        builder.field("target_field", "ip-city");
                        builder.field("database_file", "MyCustomGeoLite2-City.mmdb");
                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder.endArray();
        }));
    }

    private void putNonGeoipPipeline(String pipelineId) throws IOException {
        putJsonPipeline(pipelineId, ((builder, params) -> {
            builder.startArray("processors");
            {
                builder.startObject();
                {
                    builder.startObject(NonGeoProcessorsPlugin.NON_GEO_PROCESSOR_TYPE);
                    builder.field("randomField", randomAlphaOfLength(20));
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.startObject(NonGeoProcessorsPlugin.NON_GEO_PROCESSOR_TYPE);
                    builder.field("randomField", randomAlphaOfLength(20));
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.startObject(NonGeoProcessorsPlugin.NON_GEO_PROCESSOR_TYPE);
                    builder.field("randomField", randomAlphaOfLength(20));
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder.endArray();
        }));
    }

    private void deleteDatabasesInConfigDirectory() throws Exception {
        org.elasticsearch.core.IOUtils.rm(
            java.util.stream.StreamSupport.stream(
                internalCluster().getInstances(org.elasticsearch.env.Environment.class).spliterator(),
                false
            )
                .map(org.elasticsearch.env.Environment::configDir)
                .map(path -> path.resolve("ingest-geoip"))
                .distinct()
                .toArray(java.nio.file.Path[]::new)
        );

        assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getConfigDatabases(), empty());
            }
        });
    }

    private void awaitAllNodesDownloadedDatabases() throws Exception {
        assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));

            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(
                    nodeResponse.getDatabases(),
                    containsInAnyOrder("GeoLite2-Country.mmdb", "GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb", "MyCustomGeoLite2-City.mmdb")
                );
            }
        });
    }

    private GeoIpTaskState getGeoIpTaskState() {
        PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
        assertNotNull(task);
        GeoIpTaskState state = (GeoIpTaskState) task.getState();
        assertNotNull(state);
        return state;
    }

    private PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> getTask() {
        return PersistentTasksCustomMetadata.getTaskWithId(clusterService().state(), GeoIpDownloader.GEOIP_DOWNLOADER);
    }

    public static final class NonGeoProcessorsPlugin extends Plugin implements IngestPlugin {
        public static final String NON_GEO_PROCESSOR_TYPE = "test";

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> procMap = new HashMap<>();
            procMap.put(NON_GEO_PROCESSOR_TYPE, (factories, tag, description, config, projectId) -> {
                readStringProperty(NON_GEO_PROCESSOR_TYPE, tag, config, "randomField");
                return new AbstractProcessor(tag, description) {
                    @Override
                    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                        config.remove("randomField");
                    }

                    @Override
                    public String getType() {
                        return NON_GEO_PROCESSOR_TYPE;
                    }
                };
            });
            return procMap;
        }
    }
}
