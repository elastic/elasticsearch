/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoIpDownloaderIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ReindexPlugin.class,
            IngestGeoIpPlugin.class,
            GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class,
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
        deleteDatabasesInConfigDirectory();

        updateClusterSettings(
            Settings.builder()
                .putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey())
                .putNull(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey())
                .putNull("ingest.geoip.database_validity")
        );
        assertBusy(() -> {
            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
            if (task != null) {
                GeoIpTaskState state = (GeoIpTaskState) task.getState();
                assertThat(state.getDatabases(), anEmptyMap());
            }
        });
        assertBusy(() -> {
            GeoIpDownloaderStatsAction.Response response = client().execute(
                GeoIpDownloaderStatsAction.INSTANCE,
                new GeoIpDownloaderStatsAction.Request()
            ).actionGet();
            assertThat(response.getStats().getDatabasesCount(), equalTo(0));
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpDownloaderStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getConfigDatabases(), empty());
                assertThat(nodeResponse.getDatabases(), empty());
                assertThat(nodeResponse.getFilesInTemp().stream().filter(s -> s.endsWith(".txt") == false).toList(), empty());
            }
        });
        assertBusy(() -> {
            List<Path> geoIpTmpDirs = getGeoIpTmpDirs();
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(names, not(hasItem("GeoLite2-ASN.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-City.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-Country.mmdb")));
                    assertThat(names, not(hasItem("MyCustomGeoLite2-City.mmdb")));
                }
            }
        });
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/75221")
    public void testInvalidTimestamp() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        setupDatabasesInConfigDirectory();
        putGeoIpPipeline();
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(
                Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb"),
                state.getDatabases().keySet()
            );
        }, 2, TimeUnit.MINUTES);

        putGeoIpPipeline();
        verifyUpdatedDatabase();

        updateClusterSettings(Settings.builder().put("ingest.geoip.database_validity", TimeValue.timeValueMillis(1)));
        updateClusterSettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2))
        );
        List<Path> geoIpTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(names, not(hasItem("GeoLite2-ASN.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-City.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-Country.mmdb")));
                    assertThat(names, not(hasItem("MyCustomGeoLite2-City.mmdb")));
                }
            }
        });
        putGeoIpPipeline();
        assertBusy(() -> {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertTrue(result.getIngestDocument().hasField("tags"));
            @SuppressWarnings("unchecked")
            List<String> tags = result.getIngestDocument().getFieldValue("tags", List.class);
            // We are provided 4 databases from the download service fixture. 3 of those db files are "stock" in that we provide them
            // via the elastic download service. We also have those 3 db files set in this test as locally configured db files which are
            // "expired". The 4th is only present when the downloader has executed. When we mark all db's as expired, the downloaded copies
            // are purged, but the 3 local files remain. Processors using the local db files will tag documents with the expired database
            // tag. The processor that expects the custom database cannot find one anywhere, so it tags documents with the unavailable
            // database tag.
            assertThat(tags, contains("_geoip_expired_database", "_geoip_database_unavailable_MyCustomGeoLite2-City.mmdb"));
            assertFalse(result.getIngestDocument().hasField("ip-city"));
            assertFalse(result.getIngestDocument().hasField("ip-asn"));
            assertFalse(result.getIngestDocument().hasField("ip-country"));
        });
        updateClusterSettings(Settings.builder().putNull("ingest.geoip.database_validity"));
        assertBusy(() -> {
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(
                        names,
                        hasItems("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
                    );
                }
            }
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92888")
    public void testUpdatedTimestamp() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        testGeoIpDatabasesDownload();
        long lastCheck = getGeoIpTaskState().getDatabases().get("GeoLite2-ASN.mmdb").lastCheck();
        updateClusterSettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2))
        );
        assertBusy(() -> assertNotEquals(lastCheck, getGeoIpTaskState().getDatabases().get("GeoLite2-ASN.mmdb").lastCheck()));
        testGeoIpDatabasesDownload();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92888")
    public void testGeoIpDatabasesDownload() throws Exception {
        putGeoIpPipeline();
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(
                Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb"),
                state.getDatabases().keySet()
            );
            putGeoIpPipeline(); // This is to work around the race condition described in #92888
        }, 2, TimeUnit.MINUTES);

        for (String id : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")) {
            assertBusy(() -> {
                try {
                    GeoIpTaskState state = (GeoIpTaskState) getTask().getState();
                    assertEquals(
                        Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb"),
                        state.getDatabases().keySet()
                    );
                    GeoIpTaskState.Metadata metadata = state.get(id);
                    int size = metadata.lastChunk() - metadata.firstChunk() + 1;
                    assertResponse(
                        prepareSearch(GeoIpDownloader.DATABASES_INDEX).setSize(size)
                            .setQuery(
                                new BoolQueryBuilder().filter(new MatchQueryBuilder("name", id))
                                    .filter(new RangeQueryBuilder("chunk").from(metadata.firstChunk()).to(metadata.lastChunk(), true))
                            )
                            .addSort("chunk", SortOrder.ASC),
                        res -> {
                            try {
                                TotalHits totalHits = res.getHits().getTotalHits();
                                assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation);
                                assertEquals(size, totalHits.value);
                                assertEquals(size, res.getHits().getHits().length);

                                List<byte[]> data = new ArrayList<>();

                                for (SearchHit hit : res.getHits().getHits()) {
                                    data.add((byte[]) hit.getSourceAsMap().get("data"));
                                }

                                TarInputStream stream = new TarInputStream(new GZIPInputStream(new MultiByteArrayInputStream(data)));
                                TarInputStream.TarEntry entry;
                                while ((entry = stream.getNextEntry()) != null) {
                                    if (entry.name().endsWith(".mmdb")) {
                                        break;
                                    }
                                }

                                Path tempFile = createTempFile();
                                Files.copy(stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                                parseDatabase(tempFile);
                            } catch (Exception e) {
                                fail(e);
                            }
                        }
                    );
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
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
        putNonGeoipPipeline(pipelineId);
        assertNotNull(getTask().getState()); // removing all geoip processors should not result in the task being stopped
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(
                Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb"),
                state.getDatabases().keySet()
            );
        });
    }

    public void testDoNotDownloadDatabaseOnPipelineCreation() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String pipelineId = randomIdentifier();

        // Removing databases from tmp dir. So we can test the downloader.
        deleteDatabasesInConfigDirectory();

        // Enabling the downloader.
        putGeoIpPipeline("_id", false);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> assertNotNull(getTask()));

        // Creating a pipeline containing a geo processor with lazy download enable.
        // Download should not be triggered and task state should stay null.
        putGeoIpPipeline(pipelineId, false);
        assertNull(getTask().getState());

        // Creating an index which does not reference the pipeline should not trigger the database download.
        String indexIdentifier = randomIdentifier();
        assertAcked(indicesAdmin().prepareCreate(indexIdentifier).get());
        assertNull(getTask().getState());

        // Set the pipeline as default_pipeline or final_pipeline for the index.
        // This should trigger the database download.
        Setting<String> pipelineSetting = randomFrom(IndexSettings.FINAL_PIPELINE, IndexSettings.DEFAULT_PIPELINE);
        Settings indexSettings = Settings.builder().put(pipelineSetting.getKey(), pipelineId).build();
        assertAcked(indicesAdmin().prepareUpdateSettings(indexIdentifier).setSettings(indexSettings).get());
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(
                Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb"),
                state.getDatabases().keySet()
            );
        }, 2, TimeUnit.MINUTES);

        // Remove the created index.
        assertAcked(indicesAdmin().prepareDelete(indexIdentifier).get());
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/69972")
    public void testUseGeoIpProcessorWithDownloadedDBs() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        setupDatabasesInConfigDirectory();
        // setup:
        putGeoIpPipeline();

        // verify before updating dbs
        {
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
        }

        // Enable downloader:
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<Path> geoipTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::getFileName).map(Path::toString).toList();
                    assertThat(
                        files,
                        containsInAnyOrder(
                            "GeoLite2-City.mmdb",
                            "GeoLite2-Country.mmdb",
                            "GeoLite2-ASN.mmdb",
                            "MyCustomGeoLite2-City.mmdb",
                            "GeoLite2-City.mmdb_COPYRIGHT.txt",
                            "GeoLite2-Country.mmdb_COPYRIGHT.txt",
                            "GeoLite2-ASN.mmdb_COPYRIGHT.txt",
                            "MyCustomGeoLite2-City.mmdb_COPYRIGHT.txt",
                            "GeoLite2-City.mmdb_LICENSE.txt",
                            "GeoLite2-Country.mmdb_LICENSE.txt",
                            "GeoLite2-ASN.mmdb_LICENSE.txt",
                            "GeoLite2-ASN.mmdb_README.txt",
                            "MyCustomGeoLite2-City.mmdb_LICENSE.txt"
                        )
                    );
                }
            }
        }, 20, TimeUnit.SECONDS);

        verifyUpdatedDatabase();

        // Disable downloader:
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false));

        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::toString).filter(p -> p.endsWith(".mmdb")).toList();
                    assertThat(files, empty());
                }
            }
        });
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/79074")
    public void testStartWithNoDatabases() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        putGeoIpPipeline();

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
        verifyUpdatedDatabase();
    }

    private void verifyUpdatedDatabase() throws Exception {
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

    private GeoIpTaskState getGeoIpTaskState() {
        PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
        assertNotNull(task);
        GeoIpTaskState state = (GeoIpTaskState) task.getState();
        assertNotNull(state);
        return state;
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
        SimulatePipelineRequest simulateRequest = new SimulatePipelineRequest(bytes, XContentType.JSON);
        simulateRequest.setId("_id");
        // Avoid executing on a coordinating only node, because databases are not available there and geoip processor won't do any lookups.
        // (some test seeds repeatedly hit such nodes causing failures)
        Client client = dataNodeClient();
        SimulatePipelineResponse simulateResponse = client.admin().cluster().simulatePipeline(simulateRequest).actionGet();
        assertThat(simulateResponse.getPipelineId(), equalTo("_id"));
        assertThat(simulateResponse.getResults().size(), equalTo(1));
        return (SimulateDocumentBaseResult) simulateResponse.getResults().get(0);
    }

    /**
     * This creates a pipeline with a geoip processor, which ought to cause the geoip downloader to begin (assuming it is enabled).
     * @throws IOException
     */
    private void putGeoIpPipeline() throws IOException {
        putGeoIpPipeline("_id");
    }

    /**
     * This creates a pipeline named pipelineId with a geoip processor, which ought to cause the geoip downloader to begin (assuming it is
     * enabled).
     * @param pipelineId The name of the new pipeline with a geoip processor
     * @throws IOException
     */
    private void putGeoIpPipeline(String pipelineId) throws IOException {
        putGeoIpPipeline(pipelineId, true);
    }

    /**
     * This creates a pipeline named pipelineId with a geoip processor, which ought to cause the geoip downloader to begin (assuming it is
     * enabled).
     * @param pipelineId The name of the new pipeline with a geoip processor
     * @param downloadDatabaseOnPipelineCreation Indicates whether the pipeline creation should trigger database download or not.
     * @throws IOException
     */
    private void putGeoIpPipeline(String pipelineId, boolean downloadDatabaseOnPipelineCreation) throws IOException {
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    /*
                     * First we add a non-geo pipeline with a random field value. This is purely here so that each call to this method
                     * creates a pipeline that is unique. Creating the a pipeline twice with the same ID and exact same bytes
                     * results in a no-op, meaning that the pipeline won't actually be updated and won't actually trigger all of the
                     * things we expect it to.
                     */
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
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(clusterAdmin().preparePutPipeline(pipelineId, bytes, XContentType.JSON).get());
    }

    /**
     * This creates a pipeline named pipelineId that does _not_ have a geoip processor.
     * @throws IOException
     */
    private void putNonGeoipPipeline(String pipelineId) throws IOException {
        /*
         * Adding the exact same pipeline twice is treated as a no-op. The random values that go into randomField make each pipeline
         * created by this method is unique to avoid this.
         */
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
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
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(clusterAdmin().preparePutPipeline(pipelineId, bytes, XContentType.JSON).get());
    }

    private List<Path> getGeoIpTmpDirs() throws IOException {
        final Set<String> ids = clusterService().state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
        // All nodes share the same geoip base dir in the shared tmp dir:
        Path geoipBaseTmpDir = internalCluster().getDataNodeInstance(Environment.class).tmpFile().resolve("geoip-databases");
        assertThat(Files.exists(geoipBaseTmpDir), is(true));
        final List<Path> geoipTmpDirs;
        try (Stream<Path> files = Files.list(geoipBaseTmpDir)) {
            geoipTmpDirs = files.filter(path -> ids.contains(path.getFileName().toString())).toList();
        }
        assertThat(geoipTmpDirs.size(), equalTo(internalCluster().numDataNodes()));
        return geoipTmpDirs;
    }

    private void setupDatabasesInConfigDirectory() throws Exception {
        StreamSupport.stream(internalCluster().getInstances(Environment.class).spliterator(), false)
            .map(Environment::configFile)
            .map(path -> path.resolve("ingest-geoip"))
            .distinct()
            .forEach(path -> {
                try {
                    Files.createDirectories(path);
                    Files.copy(GeoIpDownloaderIT.class.getResourceAsStream("/GeoLite2-City.mmdb"), path.resolve("GeoLite2-City.mmdb"));
                    Files.copy(GeoIpDownloaderIT.class.getResourceAsStream("/GeoLite2-ASN.mmdb"), path.resolve("GeoLite2-ASN.mmdb"));
                    Files.copy(
                        GeoIpDownloaderIT.class.getResourceAsStream("/GeoLite2-Country.mmdb"),
                        path.resolve("GeoLite2-Country.mmdb")
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

        assertBusy(() -> {
            GeoIpDownloaderStatsAction.Response response = client().execute(
                GeoIpDownloaderStatsAction.INSTANCE,
                new GeoIpDownloaderStatsAction.Request()
            ).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpDownloaderStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(
                    nodeResponse.getConfigDatabases(),
                    containsInAnyOrder("GeoLite2-Country.mmdb", "GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb")
                );
                assertThat(nodeResponse.getDatabases(), empty());
                assertThat(nodeResponse.getFilesInTemp().stream().filter(s -> s.endsWith(".txt") == false).toList(), empty());
            }
        });
    }

    private void deleteDatabasesInConfigDirectory() throws Exception {
        StreamSupport.stream(internalCluster().getInstances(Environment.class).spliterator(), false)
            .map(Environment::configFile)
            .map(path -> path.resolve("ingest-geoip"))
            .distinct()
            .forEach(path -> {
                try {
                    IOUtils.rm(path);
                } catch (IOException e) {
                    /*
                     * If the test is emulating Windows mode then it will throw an IOException if something has an open file handle to this
                     * directory. ConfigDatabases adds a FileWatcher that lists the contents of this directory every 5 seconds. If the
                     * timing is unlucky a directory listing can happen just as we are attempting to do this delete. In that case we wait a
                     * small amount of time and retry once. If it fails a second time then something more serious is going on so we bail
                     * out.
                     */
                    if (path.getFileSystem().provider().getScheme().equals("windows://") && e.getMessage().contains("access denied")) {
                        logger.debug("Caught an IOException, will sleep and try deleting again", e);
                        safeSleep(500);
                        try {
                            IOUtils.rm(path);
                        } catch (IOException e2) {
                            throw new UncheckedIOException(e2);
                        }
                    } else {
                        throw new UncheckedIOException(e);
                    }

                }
            });

        assertBusy(() -> {
            GeoIpDownloaderStatsAction.Response response = client().execute(
                GeoIpDownloaderStatsAction.INSTANCE,
                new GeoIpDownloaderStatsAction.Request()
            ).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpDownloaderStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getConfigDatabases(), empty());
            }
        });
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private void parseDatabase(Path tempFile) throws IOException {
        try (DatabaseReader databaseReader = new DatabaseReader.Builder(tempFile.toFile()).build()) {
            assertNotNull(databaseReader.getMetadata());
        }
    }

    private PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> getTask() {
        return PersistentTasksCustomMetadata.getTaskWithId(clusterService().state(), GeoIpDownloader.GEOIP_DOWNLOADER);
    }

    private static class MultiByteArrayInputStream extends InputStream {

        private final Iterator<byte[]> data;
        private ByteArrayInputStream current;

        private MultiByteArrayInputStream(List<byte[]> data) {
            this.data = data.iterator();
        }

        @Override
        public int read() {
            if (current == null) {
                if (data.hasNext() == false) {
                    return -1;
                }

                current = new ByteArrayInputStream(data.next());
            }
            int read = current.read();
            if (read == -1) {
                current = null;
                return read();
            }
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (current == null) {
                if (data.hasNext() == false) {
                    return -1;
                }

                current = new ByteArrayInputStream(data.next());
            }
            int read = current.read(b, off, len);
            if (read == -1) {
                current = null;
                return read(b, off, len);
            }
            return read;
        }
    }

    /**
     * This class defines a processor of type "test".
     */
    public static final class NonGeoProcessorsPlugin extends Plugin implements IngestPlugin {
        /*
         * This processor has a single field, randomField. Its sole purpose is to hold a random value to make the processor unique from
         * other prorcessors that are otherwise identical. The only reason for this is so that the pipeline the processor belongs to is
         * unique. And the only reason for that is so that adding the pipeline is not treated as a no-op.
         */
        public static final String NON_GEO_PROCESSOR_TYPE = "test";

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> procMap = new HashMap<>();
            procMap.put(NON_GEO_PROCESSOR_TYPE, (factories, tag, description, config) -> {
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
