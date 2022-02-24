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
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    protected static final String ENDPOINT = System.getProperty("geoip_endpoint");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestGeoIpPlugin.class, GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (ENDPOINT != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), ENDPOINT);
        }
        return settings.build();
    }

    @After
    public void cleanUp() throws Exception {
        deleteDatabasesInConfigDirectory();

        ClusterUpdateSettingsResponse settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey())
                    .putNull(GeoIpDownloader.POLL_INTERVAL_SETTING.getKey())
                    .putNull("ingest.geoip.database_validity")
            )
            .get();
        assertTrue(settingsResponse.isAcknowledged());

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
                assertThat(
                    nodeResponse.getFilesInTemp().stream().filter(s -> s.endsWith(".txt") == false).collect(Collectors.toList()),
                    empty()
                );
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
                }
            }
        });
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/75221")
    public void testInvalidTimestamp() throws Exception {
        assumeTrue("only test with fixture to have stable results", ENDPOINT != null);
        ClusterUpdateSettingsResponse settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"), state.getDatabases().keySet());
        }, 2, TimeUnit.MINUTES);

        putPipeline();
        verifyUpdatedDatabase();

        settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("ingest.geoip.database_validity", TimeValue.timeValueMillis(1)))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloader.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2)))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        List<Path> geoIpTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(names, not(hasItem("GeoLite2-ASN.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-City.mmdb")));
                    assertThat(names, not(hasItem("GeoLite2-Country.mmdb")));
                }
            }
        });
        putPipeline();
        assertBusy(() -> {
            SimulateDocumentBaseResult result = simulatePipeline();
            assertThat(result.getFailure(), nullValue());
            assertTrue(result.getIngestDocument().hasField("tags"));
            @SuppressWarnings("unchecked")
            List<String> tags = result.getIngestDocument().getFieldValue("tags", List.class);
            assertThat(tags, contains("_geoip_expired_database"));
            assertFalse(result.getIngestDocument().hasField("ip-city"));
            assertFalse(result.getIngestDocument().hasField("ip-asn"));
            assertFalse(result.getIngestDocument().hasField("ip-country"));
        });
        settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull("ingest.geoip.database_validity"))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        assertBusy(() -> {
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(names, hasItems("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"));
                }
            }
        });
    }

    public void testUpdatedTimestamp() throws Exception {
        assumeTrue("only test with fixture to have stable results", ENDPOINT != null);
        testGeoIpDatabasesDownload();
        long lastCheck = getGeoIpTaskState().getDatabases().get("GeoLite2-ASN.mmdb").lastCheck();
        ClusterUpdateSettingsResponse settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloader.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2)))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        assertBusy(() -> assertNotEquals(lastCheck, getGeoIpTaskState().getDatabases().get("GeoLite2-ASN.mmdb").lastCheck()));
        testGeoIpDatabasesDownload();
    }

    public void testGeoIpDatabasesDownload() throws Exception {
        ClusterUpdateSettingsResponse settingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertEquals(Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"), state.getDatabases().keySet());
        }, 2, TimeUnit.MINUTES);

        for (String id : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
            assertBusy(() -> {
                try {
                    GeoIpTaskState state = (GeoIpTaskState) getTask().getState();
                    assertEquals(Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"), state.getDatabases().keySet());
                    GeoIpTaskState.Metadata metadata = state.get(id);
                    BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(new MatchQueryBuilder("name", id))
                        .filter(new RangeQueryBuilder("chunk").from(metadata.firstChunk()).to(metadata.lastChunk(), true));
                    int size = metadata.lastChunk() - metadata.firstChunk() + 1;
                    SearchResponse res = client().prepareSearch(GeoIpDownloader.DATABASES_INDEX)
                        .setSize(size)
                        .setQuery(queryBuilder)
                        .addSort("chunk", SortOrder.ASC)
                        .get();
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
                    throw new AssertionError(e);
                }
            });
        }
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/69972")
    public void testUseGeoIpProcessorWithDownloadedDBs() throws Exception {
        assumeTrue("only test with fixture to have stable results", ENDPOINT != null);
        setupDatabasesInConfigDirectory();
        // setup:
        putPipeline();

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
        Settings.Builder settings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));

        final List<Path> geoipTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::getFileName).map(Path::toString).collect(Collectors.toList());
                    assertThat(
                        files,
                        containsInAnyOrder(
                            "GeoLite2-City.mmdb",
                            "GeoLite2-Country.mmdb",
                            "GeoLite2-ASN.mmdb",
                            "GeoLite2-City.mmdb_COPYRIGHT.txt",
                            "GeoLite2-Country.mmdb_COPYRIGHT.txt",
                            "GeoLite2-ASN.mmdb_COPYRIGHT.txt",
                            "GeoLite2-City.mmdb_LICENSE.txt",
                            "GeoLite2-Country.mmdb_LICENSE.txt",
                            "GeoLite2-ASN.mmdb_LICENSE.txt",
                            "GeoLite2-ASN.mmdb_README.txt"
                        )
                    );
                }
            }
        });

        verifyUpdatedDatabase();

        // Disable downloader:
        settings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));

        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::toString).filter(p -> p.endsWith(".mmdb")).collect(Collectors.toList());
                    assertThat(files, empty());
                }
            }
        });
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/79074")
    public void testStartWithNoDatabases() throws Exception {
        assumeTrue("only test with fixture to have stable results", ENDPOINT != null);
        putPipeline();

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
                        "_geoip_database_unavailable_GeoLite2-ASN.mmdb"
                    )
                )
            );
        }

        // Enable downloader:
        Settings.Builder settings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
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

    private void putPipeline() throws IOException {
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
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
        assertAcked(client().admin().cluster().preparePutPipeline("_id", bytes, XContentType.JSON).get());
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
            geoipTmpDirs = files.filter(path -> ids.contains(path.getFileName().toString())).collect(Collectors.toList());
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
                assertThat(
                    nodeResponse.getFilesInTemp().stream().filter(s -> s.endsWith(".txt") == false).collect(Collectors.toList()),
                    empty()
                );
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
}
