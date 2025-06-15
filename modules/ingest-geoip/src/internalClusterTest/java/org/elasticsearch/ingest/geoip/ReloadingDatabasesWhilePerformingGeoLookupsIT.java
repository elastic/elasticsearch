/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.ingest.geoip.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(
    value = {
        "ExtrasFS", // Don't randomly add 'extra' files to directory.
        "WindowsFS" // Files.copy(...) replaces files being in use and causes 'java.io.IOException: access denied: ...' mock errors (from
                    // 'WindowsFS.checkDeleteAccess(...)').
    }
)
public class ReloadingDatabasesWhilePerformingGeoLookupsIT extends ESTestCase {

    /**
     * This tests essentially verifies that a Maxmind database reader doesn't fail with:
     * com.maxmind.db.ClosedDatabaseException: The MaxMind DB has been closed
     *
     * This failure can be avoided by ensuring that a database is only closed when no
     * geoip processor instance is using the related {@link DatabaseReaderLazyLoader} instance
     */
    public void test() throws Exception {
        Path geoIpConfigDir = createTempDir();
        Path geoIpTmpDir = createTempDir();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        DatabaseNodeService databaseNodeService = createRegistry(geoIpConfigDir, geoIpTmpDir, clusterService);
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(GEOIP_TYPE, databaseNodeService);
        copyDatabase("GeoLite2-City-Test.mmdb", geoIpTmpDir.resolve("GeoLite2-City.mmdb"));
        copyDatabase("GeoLite2-City-Test.mmdb", geoIpTmpDir.resolve("GeoLite2-City-Test.mmdb"));
        databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoIpTmpDir.resolve("GeoLite2-City.mmdb"));
        databaseNodeService.updateDatabase("GeoLite2-City-Test.mmdb", "md5", geoIpTmpDir.resolve("GeoLite2-City-Test.mmdb"));
        lazyLoadReaders(databaseNodeService);

        final GeoIpProcessor processor1 = (GeoIpProcessor) factory.create(
            null,
            "_tag",
            null,
            new HashMap<>(Map.of("field", "_field")),
            null
        );
        final GeoIpProcessor processor2 = (GeoIpProcessor) factory.create(
            null,
            "_tag",
            null,
            new HashMap<>(Map.of("field", "_field", "database_file", "GeoLite2-City-Test.mmdb")),
            null
        );

        final AtomicBoolean completed = new AtomicBoolean(false);
        final int numberOfDatabaseUpdates = randomIntBetween(2, 4);
        final AtomicInteger numberOfIngestRuns = new AtomicInteger();
        final int numberOfIngestThreads = randomIntBetween(16, 32);
        final Thread[] ingestThreads = new Thread[numberOfIngestThreads];
        final AtomicArray<Throwable> ingestFailures = new AtomicArray<>(numberOfIngestThreads);
        for (int i = 0; i < numberOfIngestThreads; i++) {
            final int id = i;
            ingestThreads[id] = new Thread(() -> {
                while (completed.get() == false) {
                    try {
                        IngestDocument document1 = new IngestDocument(
                            "index",
                            "id",
                            1L,
                            "routing",
                            VersionType.EXTERNAL,
                            new HashMap<>(Map.of("_field", "89.160.20.128"))
                        );
                        processor1.execute(document1);
                        assertThat(document1.getSourceAndMetadata().get("geoip"), notNullValue());
                        IngestDocument document2 = new IngestDocument(
                            "index",
                            "id",
                            1L,
                            "routing",
                            VersionType.EXTERNAL,
                            new HashMap<>(Map.of("_field", "89.160.20.128"))
                        );
                        processor2.execute(document2);
                        assertThat(document2.getSourceAndMetadata().get("geoip"), notNullValue());
                        numberOfIngestRuns.incrementAndGet();
                    } catch (Exception | AssertionError e) {
                        logger.error("error in ingest thread after run [" + numberOfIngestRuns.get() + "]", e);
                        ingestFailures.setOnce(id, e);
                        break;
                    }
                }
            });
        }

        final AtomicReference<Throwable> failureHolder2 = new AtomicReference<>();
        Thread updateDatabaseThread = new Thread(() -> {
            for (int i = 0; i < numberOfDatabaseUpdates; i++) {
                try {
                    DatabaseReaderLazyLoader previous1 = databaseNodeService.get("GeoLite2-City.mmdb");
                    if (Files.exists(geoIpTmpDir.resolve("GeoLite2-City.mmdb"))) {
                        databaseNodeService.removeStaleEntries(List.of("GeoLite2-City.mmdb"));
                        assertBusy(() -> {
                            // lazy loader may still be in use by an ingest thread,
                            // wait for any potential ingest thread to release the lazy loader (DatabaseReaderLazyLoader#postLookup(...)),
                            // this will do clean it up and actually deleting the underlying file
                            assertThat(Files.exists(geoIpTmpDir.resolve("GeoLite2-City.mmdb")), is(false));
                            assertThat(previous1.current(), equalTo(-1));
                        });
                    } else {
                        copyDatabase("GeoLite2-City-Test.mmdb", geoIpTmpDir.resolve("GeoLite2-City.mmdb"));
                        databaseNodeService.updateDatabase("GeoLite2-City.mmdb", "md5", geoIpTmpDir.resolve("GeoLite2-City.mmdb"));
                    }
                    DatabaseReaderLazyLoader previous2 = databaseNodeService.get("GeoLite2-City-Test.mmdb");
                    copyDatabase(
                        i % 2 == 0 ? "GeoIP2-City-Test.mmdb" : "GeoLite2-City-Test.mmdb",
                        geoIpTmpDir.resolve("GeoLite2-City-Test.mmdb")
                    );
                    databaseNodeService.updateDatabase("GeoLite2-City-Test.mmdb", "md5", geoIpTmpDir.resolve("GeoLite2-City-Test.mmdb"));

                    DatabaseReaderLazyLoader current1 = databaseNodeService.get("GeoLite2-City.mmdb");
                    DatabaseReaderLazyLoader current2 = databaseNodeService.get("GeoLite2-City-Test.mmdb");
                    assertThat(current1, not(sameInstance(previous1)));
                    assertThat(current2, not(sameInstance(previous2)));

                    // lazy load type and reader:
                    lazyLoadReaders(databaseNodeService);
                } catch (Exception | AssertionError e) {
                    logger.error("error in update databases thread after run [" + i + "]", e);
                    failureHolder2.set(e);
                    break;
                }
            }
            completed.set(true);
        });

        Arrays.stream(ingestThreads).forEach(Thread::start);
        updateDatabaseThread.start();
        Arrays.stream(ingestThreads).forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        updateDatabaseThread.join();

        ingestFailures.asList().forEach(r -> assertThat(r, nullValue()));
        assertThat(failureHolder2.get(), nullValue());
        assertThat(numberOfIngestRuns.get(), greaterThan(0));

        for (DatabaseReaderLazyLoader lazyLoader : databaseNodeService.getAllDatabases()) {
            assertThat(lazyLoader.current(), equalTo(0));
        }
        // Avoid accumulating many temp dirs while running with -Dtests.iters=X
        IOUtils.rm(geoIpConfigDir, geoIpTmpDir);
    }

    private static DatabaseNodeService createRegistry(Path geoIpConfigDir, Path geoIpTmpDir, ClusterService clusterService)
        throws IOException {
        GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(0);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);
        DatabaseNodeService databaseNodeService = new DatabaseNodeService(
            geoIpTmpDir,
            mock(Client.class),
            cache,
            configDatabases,
            Runnable::run,
            clusterService
        );
        databaseNodeService.initialize("nodeId", mock(ResourceWatcherService.class), mock(IngestService.class));
        return databaseNodeService;
    }

    private static void lazyLoadReaders(DatabaseNodeService databaseNodeService) throws IOException {
        if (databaseNodeService.get("GeoLite2-City.mmdb") != null) {
            databaseNodeService.get("GeoLite2-City.mmdb").getDatabaseType();
            databaseNodeService.get("GeoLite2-City.mmdb").getResponse("2.125.160.216", GeoIpTestUtils::getCity);
        }
        databaseNodeService.get("GeoLite2-City-Test.mmdb").getDatabaseType();
        databaseNodeService.get("GeoLite2-City-Test.mmdb").getResponse("2.125.160.216", GeoIpTestUtils::getCity);
    }

}
