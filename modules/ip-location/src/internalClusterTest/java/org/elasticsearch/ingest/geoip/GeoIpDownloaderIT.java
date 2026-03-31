/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.Reader;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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
            IngestGeoIpSettingsPlugin.class,
            GeoIpIndexSettingProviderPlugin.class
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
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getDownloaderStats().getDatabasesCount(), equalTo(0));
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
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
        String projectId = ProjectId.DEFAULT.id();
        setupDatabasesInConfigDirectory();
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
        }, 2, TimeUnit.MINUTES);

        verifyUpdatedDatabase(projectId);
        awaitAllNodesDownloadedDatabases();

        updateClusterSettings(Settings.builder().put("ingest.geoip.database_validity", TimeValue.timeValueMillis(1)));
        updateClusterSettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(2))
        );
        List<Path> geoIpTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoIpTmpDir : geoIpTmpDirs) {
                try (Stream<Path> files = Files.list(geoIpTmpDir)) {
                    Set<String> names = files.map(f -> f.getFileName().toString()).collect(Collectors.toSet());
                    assertThat(
                        names,
                        allOf(
                            not(hasItem("GeoLite2-ASN.mmdb")),
                            not(hasItem("GeoLite2-City.mmdb")),
                            not(hasItem("GeoLite2-Country.mmdb")),
                            not(hasItem("MyCustomGeoLite2-City.mmdb"))
                        )
                    );
                }
            }
        });
        // We wait for the deletion of the database chunks in the .geoip_databases index. Since the search request cache is disabled by
        // GeoIpIndexSettingProvider, we can be sure that all nodes are unable to fetch the deleted chunks from the cache.
        logger.info("---> waiting for database chunks to be deleted");
        assertBusy(() -> assertNoSearchHits(prepareSearch(GeoIpDownloader.DATABASES_INDEX).setRequestCache(false)));
        logger.info("---> database chunks deleted");
        assertBusy(() -> {
            // After expiration, downloaded databases are purged. The 3 config databases remain but are expired,
            // and the custom database (only available via download) is gone entirely.
            IpLocationService service = internalCluster().getAnyMasterNodeInstance(IpLocationService.class);
            IpDataLookup cityLookup = service.createIpDataLookup(projectId, "GeoLite2-City.mmdb", null);
            assertThat("expired config databases should still resolve to a lookup handle", cityLookup, notNullValue());
            assertThat("expired database should not return data", cityLookup.isValid(), is(false));
            IpDataLookup customLookup = service.createIpDataLookup(projectId, "MyCustomGeoLite2-City.mmdb", null);
            assertThat("custom database should be unavailable after expiration", customLookup, nullValue());
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
        IpLocationTestHelper.requestDownloads(internalCluster(), ProjectId.DEFAULT.id());
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertBusy(() -> {
            GeoIpTaskState state = getGeoIpTaskState();
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
            IpLocationTestHelper.requestDownloads(internalCluster(), ProjectId.DEFAULT.id());
        }, 2, TimeUnit.MINUTES);

        for (String id : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")) {
            assertBusy(() -> {
                try {
                    GeoIpTaskState state = (GeoIpTaskState) getTask().getState();
                    assertThat(
                        state.getDatabases().keySet(),
                        containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
                    );
                    GeoIpTaskState.Metadata metadata = state.getDatabases().get(id);
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
                                assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation());
                                assertEquals(size, totalHits.value());
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

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/69972")
    public void testUseGeoIpProcessorWithDownloadedDBs() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String projectId = ProjectId.DEFAULT.id();
        setupDatabasesInConfigDirectory();

        // Verify config databases are available before download
        {
            assertBusy(() -> {
                IpLocationTestHelper.assertDatabaseAvailable(
                    internalCluster(),
                    projectId,
                    "GeoLite2-City.mmdb",
                    "89.160.20.128",
                    "city_name",
                    "Tumba"
                );
                IpLocationTestHelper.assertDatabaseAvailable(
                    internalCluster(),
                    projectId,
                    "GeoLite2-ASN.mmdb",
                    "89.160.20.128",
                    "organization_name",
                    "Bredband2 AB"
                );
                IpLocationTestHelper.assertDatabaseAvailable(
                    internalCluster(),
                    projectId,
                    "GeoLite2-Country.mmdb",
                    "89.160.20.128",
                    "country_name",
                    "Sweden"
                );
            });
        }

        // Enable downloader:
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));

        final List<Path> geoipTmpDirs = getGeoIpTmpDirs();
        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::getFileName).map(Path::toString).toList();
                    assertThat(
                        files,
                        containsInAnyOrder(
                            "GeoLite2-ASN.mmdb",
                            "GeoLite2-ASN.mmdb_COPYRIGHT.txt",
                            "GeoLite2-ASN.mmdb_LICENSE.txt",
                            "GeoLite2-City.mmdb",
                            "GeoLite2-City.mmdb_COPYRIGHT.txt",
                            "GeoLite2-City.mmdb_LICENSE.txt",
                            "GeoLite2-City.mmdb_README.txt",
                            "GeoLite2-Country.mmdb",
                            "GeoLite2-Country.mmdb_COPYRIGHT.txt",
                            "GeoLite2-Country.mmdb_LICENSE.txt",
                            "MyCustomGeoLite2-City.mmdb",
                            "MyCustomGeoLite2-City.mmdb_COPYRIGHT.txt",
                            "MyCustomGeoLite2-City.mmdb_LICENSE.txt"
                        )
                    );
                }
            }
        }, 20, TimeUnit.SECONDS);

        verifyUpdatedDatabase(projectId);
        awaitAllNodesDownloadedDatabases();

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

    /**
     * Demonstrates the full {@link IpLocationService} API lifecycle without any ingest pipeline dependency:
     * register listener, request downloads, enable downloader, await listener, verify lookup, disable, verify cleanup.
     */
    public void testIpLocationServiceLifecycle() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String projectId = ProjectId.DEFAULT.id();
        Set<String> expectedDatabases = Set.of(
            "GeoLite2-City.mmdb",
            "GeoLite2-ASN.mmdb",
            "GeoLite2-Country.mmdb",
            "MyCustomGeoLite2-City.mmdb"
        );

        // 1. No databases available at startup (downloader disabled, no config databases)
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-City.mmdb");
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-ASN.mmdb");

        // 2. Register a DatabaseAvailabilityListener to be notified when databases become available
        CountDownLatch databasesAvailable = new CountDownLatch(expectedDatabases.size());
        Set<String> notifiedDatabases = ConcurrentHashMap.newKeySet();
        IpLocationService listenerNode = internalCluster().getAnyMasterNodeInstance(IpLocationService.class);
        listenerNode.addDatabaseAvailabilityListener((pid, databaseFile) -> {
            if (projectId.equals(pid) && notifiedDatabases.add(databaseFile)) {
                databasesAvailable.countDown();
            }
        });

        // 3. Request downloads — flag is set on all nodes but the downloader task is not bootstrapped
        // because ENABLED=false, so no downloads can occur
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId);
        for (DatabaseNodeService dns : internalCluster().getInstances(DatabaseNodeService.class)) {
            assertTrue("downloadRequested should be true after requestDownloads()", dns.isDownloadRequested(ProjectId.DEFAULT));
        }
        assertNull("downloader task should not be bootstrapped when disabled", getTask());

        // 4. Enable downloader — task is bootstrapped, databases are downloaded, listener fires for each
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        assertTrue("timed out waiting for database availability notifications", databasesAvailable.await(2, TimeUnit.MINUTES));
        assertThat(notifiedDatabases, equalTo(expectedDatabases));
        assertNotNull("downloader task should exist after enabling", getTask());

        // 5. Verify data via createIpDataLookup + lookup
        IpLocationTestHelper.assertDatabaseAvailable(
            internalCluster(),
            projectId,
            "GeoLite2-City.mmdb",
            "89.160.20.128",
            "city_name",
            "Linköping"
        );
        IpLocationTestHelper.assertDatabaseAvailable(
            internalCluster(),
            projectId,
            "GeoLite2-ASN.mmdb",
            "89.160.20.128",
            "organization_name",
            "Bredband2 AB"
        );
        IpDataLookup cityLookup = listenerNode.createIpDataLookup(projectId, "GeoLite2-City.mmdb", null);
        assertThat(cityLookup, notNullValue());
        assertThat(cityLookup.isValid(), is(true));

        // 6. Disable downloader — triggers cleanup while downloadRequested is still true,
        // so checkDatabases() runs and removes local files when it sees no task state.
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false));

        // 7. Verify cleanup: task state cleared, databases removed from all nodes
        assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getDownloaderStats().getDatabasesCount(), equalTo(0));
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getDatabases(), empty());
            }
        });
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-City.mmdb");
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-ASN.mmdb");

        // 8. Cancel download request — resets the per-node flag after cleanup is complete
        for (IpLocationService svc : internalCluster().getInstances(IpLocationService.class)) {
            svc.cancelDownloadRequest(projectId);
        }
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/79074")
    public void testStartWithNoDatabases() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String projectId = ProjectId.DEFAULT.id();

        // Verify no databases are available before download
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-City.mmdb");
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-Country.mmdb");
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "GeoLite2-ASN.mmdb");
        IpLocationTestHelper.assertDatabaseUnavailable(internalCluster(), projectId, "MyCustomGeoLite2-City.mmdb");

        // Enable downloader:
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId);
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        verifyUpdatedDatabase(projectId);
        awaitAllNodesDownloadedDatabases();
    }

    private void verifyUpdatedDatabase(String projectId) throws Exception {
        assertBusy(() -> {
            IpLocationTestHelper.assertDatabaseAvailable(
                internalCluster(),
                projectId,
                "GeoLite2-City.mmdb",
                "89.160.20.128",
                "city_name",
                "Linköping"
            );
            IpLocationTestHelper.assertDatabaseAvailable(
                internalCluster(),
                projectId,
                "GeoLite2-ASN.mmdb",
                "89.160.20.128",
                "organization_name",
                "Bredband2 AB"
            );
            IpLocationTestHelper.assertDatabaseAvailable(
                internalCluster(),
                projectId,
                "GeoLite2-Country.mmdb",
                "89.160.20.128",
                "country_name",
                "Sweden"
            );
        });
    }

    /**
     * Waits until all ingest nodes report having downloaded the expected databases. This ensures that all ingest nodes are in a consistent
     * state and prevents us from deleting databases before they've been downloaded on all nodes.
     */
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

    private List<Path> getGeoIpTmpDirs() throws IOException {
        final Set<String> ids = clusterService().state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
        // All nodes share the same geoip base dir in the shared tmp dir:
        Path geoipBaseTmpDir = internalCluster().getDataNodeInstance(Environment.class).tmpDir().resolve("geoip-databases");
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
            .map(Environment::configDir)
            .map(path -> path.resolve("ingest-geoip"))
            .distinct()
            .forEach(path -> {
                try {
                    Files.createDirectories(path);
                    copyDefaultDatabases(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

        assertBusy(() -> {
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
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
            .map(Environment::configDir)
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
            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getConfigDatabases(), empty());
            }
        });
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private void parseDatabase(Path tempFile) throws IOException {
        try (Reader reader = new Reader(tempFile.toFile())) {
            assertNotNull(reader.getMetadata());
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
     * A simple plugin that provides the {@link GeoIpIndexSettingProvider}.
     */
    public static final class GeoIpIndexSettingProviderPlugin extends Plugin {
        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new GeoIpIndexSettingProvider());
        }
    }

    /**
     * An index setting provider that disables the request cache for the `.geoip_databases` index.
     * Since `.geoip_databases` is a system index, we can't configure this setting using the API or index templates.
     */
    public static final class GeoIpIndexSettingProvider implements IndexSettingProvider {
        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {
            if (GeoIpDownloader.GEOIP_DOWNLOADER.equals(indexName)) {
                additionalSettings.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false);
            }
        }
    }
}
