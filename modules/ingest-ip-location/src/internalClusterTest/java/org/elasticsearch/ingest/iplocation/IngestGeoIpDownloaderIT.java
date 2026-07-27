/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.AbstractGeoIpIT;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.GeoIpTaskState;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.geoip.IpLocationTestHelper;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.IngestPipelineTestUtils.jsonSimulatePipelineRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Internal cluster tests for ingest pipeline behavior with geoip processors.
 *
 * <p>Uses the default {@link ESIntegTestCase.Scope#SUITE} cluster scope so that all methods share a single test
 * cluster. The {@link #cleanUp()} hook is responsible for restoring the cluster to a pristine state between
 * methods: it nullifies the cluster settings touched by tests, deletes any ingest pipelines they created, and
 * waits for the geoip downloader persistent task to be removed. Indices are wiped automatically by
 * {@code TestCluster#wipe()}.
 */
@ESIntegTestCase.ClusterScope(maxNumDataNodes = 1)
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
    public void cleanUp() throws Exception {

        // Nullify every cluster setting touched by tests; SUITE-scoped tests assert no persistent/transient state leaks.
        updateClusterSettings(
            Settings.builder()
                .putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey())
                .putNull("ingest.geoip.database_validity")
        );

        // Tests create ingest pipelines that are not wiped by the framework's TestCluster#wipe().
        GetPipelineResponse pipelines = getPipelines("*");
        for (var pipeline : pipelines.pipelines()) {
            deletePipeline(pipeline.getId());
        }

        // Wait for the geoip downloader persistent task to be removed by its onRemove hook so the next test
        // starts without an in-flight task referencing the just-wiped .geoip_databases index.
        assertBusy(() -> assertNull(getTask()));
        IpLocationTestHelper.awaitNoDatabases(internalCluster());
        IpLocationTestHelper.deleteDatabasesInConfigDirectory(internalCluster());
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

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/69972")
    public void testUseGeoIpProcessorWithDownloadedDBs() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        IpLocationTestHelper.setupDatabasesInConfigDirectory(internalCluster());
        putGeoIpPipeline("_id");

        // verify enrichment with config databases before download
        assertBusy(() -> {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertThat(result.getIngestDocument(), notNullValue());

            IngestDocument doc = result.getIngestDocument();
            assertThat(doc.getSourceAndMetadata(), hasKey("ip-city"));
            assertThat(doc.getSourceAndMetadata(), hasKey("ip-asn"));
            assertThat(doc.getSourceAndMetadata(), hasKey("ip-country"));

            assertThat(doc.getFieldValue("ip-city.city_name", String.class), equalTo("Tumba"));
            assertThat(doc.getFieldValue("ip-asn.organization_name", String.class), equalTo("Bredband2 AB"));
            assertThat(doc.getFieldValue("ip-country.country_name", String.class), equalTo("Sweden"));
        });

        // Enable downloader — databases get updated
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        verifyEnrichment();
        awaitAllNodesDownloadedDatabases();
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/79074")
    public void testStartWithNoDatabases() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        putGeoIpPipeline("_id");

        // Behaviour without any databases loaded:
        {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertThat(result.getIngestDocument(), notNullValue());
            Map<String, Object> source = result.getIngestDocument().getSourceAndMetadata();
            assertThat(
                source,
                hasEntry(
                    "tags",
                    List.of(
                        "_geoip_database_unavailable_GeoLite2-City.mmdb",
                        "_geoip_database_unavailable_GeoLite2-Country.mmdb",
                        "_geoip_database_unavailable_GeoLite2-ASN.mmdb",
                        "_geoip_database_unavailable_MyCustomGeoLite2-City.mmdb"
                    )
                )
            );
        }

        // Enable downloader:
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        verifyEnrichment();
        awaitAllNodesDownloadedDatabases();
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/75221")
    public void testExpiredDatabases() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        IpLocationTestHelper.setupDatabasesInConfigDirectory(internalCluster());
        putGeoIpPipeline("_id");
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
        }, 2, TimeUnit.MINUTES);

        putGeoIpPipeline("_id");
        verifyEnrichment();
        awaitAllNodesDownloadedDatabases();

        updateClusterSettings(Settings.builder().put("ingest.geoip.database_validity", TimeValue.timeValueMillis(1)));
        updateClusterSettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2))
        );
        // Wait for downloaded databases to be purged from all nodes
        assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getDatabases(), empty());
            }
        });
        // Wait for database chunks to be deleted from the .geoip_databases index
        assertBusy(() -> assertNoSearchHits(prepareSearch(IpLocationTestHelper.DATABASES_INDEX).setRequestCache(false)));
        putGeoIpPipeline("_id");
        assertBusy(() -> {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertTrue(result.getIngestDocument().hasField("tags"));
            @SuppressWarnings("unchecked")
            List<String> tags = result.getIngestDocument().getFieldValue("tags", List.class);
            // The 3 config databases remain but are expired; the custom database (only available via download) is gone entirely.
            assertThat(tags, contains("_geoip_expired_database", "_geoip_database_unavailable_MyCustomGeoLite2-City.mmdb"));
            assertFalse(result.getIngestDocument().hasField("ip-city"));
            assertFalse(result.getIngestDocument().hasField("ip-asn"));
            assertFalse(result.getIngestDocument().hasField("ip-country"));
        });
    }

    public void testDoNotDownloadDatabaseOnPipelineCreation() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String pipelineId = randomIdentifier();

        IpLocationTestHelper.deleteDatabasesInConfigDirectory(internalCluster());

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

    /**
     * Verifies that deleting the last index that references a geoip pipeline does not reclaim the pipeline's databases.
     * {@code download_database_on_pipeline_creation} only governs when a database is first downloaded; it must not
     * influence reclamation. As long as a pipeline with a geoip processor exists, its databases must remain staged so
     * that the next document ingested through the pipeline is enriched immediately, rather than being silently dropped
     * (with {@code ignore_missing: true}, neither resolved nor tagged) until a re-download completes.
     *
     * <p>The pipeline here uses {@code download_database_on_pipeline_creation: false} so that retrieval is driven by an
     * index reference, and {@code ignore_missing: true} so that premature reclamation would manifest as the reported
     * silent failure.
     */
    public void testDatabasesNotReclaimedWhenReferencingIndexDeleted() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);

        IpLocationTestHelper.deleteDatabasesInConfigDirectory(internalCluster());
        putGeoIpPipeline("_id", false, true);

        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> assertNotNull(getTask()));

        // Referencing the pipeline from an index registers the ingest consumer, which triggers the download and local
        // retrieval of the databases.
        String indexIdentifier = randomIdentifier();
        assertAcked(indicesAdmin().prepareCreate(indexIdentifier).get());
        Setting<String> pipelineSetting = randomFrom(IndexSettings.FINAL_PIPELINE, IndexSettings.DEFAULT_PIPELINE);
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexIdentifier).setSettings(Settings.builder().put(pipelineSetting.getKey(), "_id")).get()
        );
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
        }, 2, TimeUnit.MINUTES);
        awaitAllNodesDownloadedDatabases();

        // Sanity check: while the index references the pipeline, enrichment works and no tags are added.
        verifyEnrichment();

        // Delete the only index referencing the pipeline. The pipeline still exists, so its databases must not be reclaimed.
        assertAcked(indicesAdmin().prepareDelete(indexIdentifier).get());

        // Assert the databases are never reclaimed: confirm "all nodes report no databases" does not become true within a
        // bounded window. expectThrows passes when assertBusy gives up (databases stay present); it fails if the databases
        // are dropped, which is the regression this test guards against.
        expectThrows(AssertionError.class, () -> assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getDatabases(), empty());
            }
        }, 10, TimeUnit.SECONDS));

        // And the next document is still enriched immediately, with no unavailability tags.
        verifyEnrichment();
    }

    private void verifyEnrichment() throws Exception {
        assertBusy(() -> {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertThat(result.getIngestDocument(), notNullValue());

            Map<?, ?> source = result.getIngestDocument().getSourceAndMetadata();
            assertThat(source, not(hasKey("tags")));
            assertThat(source, hasKey("ip-city"));
            assertThat(source, hasKey("ip-asn"));
            assertThat(source, hasKey("ip-country"));

            assertThat(((Map<?, ?>) source.get("ip-city")).get("city_name"), equalTo("Linköping"));
            assertThat(((Map<?, ?>) source.get("ip-asn")).get("organization_name"), equalTo("Bredband2 AB"));
            assertThat(((Map<?, ?>) source.get("ip-country")).get("country_name"), equalTo("Sweden"));
        });
    }

    private SimulateDocumentBaseResult simulatePipeline() throws IOException {
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startArray("docs");
            {
                builder.startObject();
                builder.field("_index", "my-index");
                {
                    builder.startObject("_source");
                    builder.field("ip", "89.160.20.128");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        var simulateRequest = jsonSimulatePipelineRequest(bytes);
        simulateRequest.setId("_id");
        // Avoid executing on a coordinating only node, because databases are not available there and geoip processor won't do any lookups.
        SimulatePipelineResponse simulateResponse = dataNodeClient().admin().cluster().simulatePipeline(simulateRequest).actionGet();
        assertThat(simulateResponse.getPipelineId(), equalTo("_id"));
        assertThat(simulateResponse.getResults().size(), equalTo(1));
        return (SimulateDocumentBaseResult) simulateResponse.getResults().get(0);
    }

    private void putGeoIpPipeline(String pipelineId) throws IOException {
        putGeoIpPipeline(pipelineId, true);
    }

    private void putGeoIpPipeline(String pipelineId, boolean downloadDatabaseOnPipelineCreation) throws IOException {
        putGeoIpPipeline(pipelineId, downloadDatabaseOnPipelineCreation, false);
    }

    private void putGeoIpPipeline(String pipelineId, boolean downloadDatabaseOnPipelineCreation, boolean ignoreMissing) throws IOException {
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
                        if (ignoreMissing) {
                            builder.field("ignore_missing", true);
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
                        if (ignoreMissing) {
                            builder.field("ignore_missing", true);
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
                        if (ignoreMissing) {
                            builder.field("ignore_missing", true);
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
                        if (ignoreMissing) {
                            builder.field("ignore_missing", true);
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

    private void awaitAllNodesDownloadedDatabases() throws Exception {
        IpLocationTestHelper.awaitAllDatabasesAvailable(internalCluster(), IpLocationConsumer.INGEST);
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
