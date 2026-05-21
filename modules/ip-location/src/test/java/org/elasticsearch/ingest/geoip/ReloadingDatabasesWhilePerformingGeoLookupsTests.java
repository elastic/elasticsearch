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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
public class ReloadingDatabasesWhilePerformingGeoLookupsTests extends ESTestCase {

    /**
     * This tests essentially verifies that a Maxmind database reader doesn't fail with:
     * com.maxmind.db.ClosedDatabaseException: The MaxMind DB has been closed
     * This failure can be avoided by ensuring that a database is only closed when no
     * geoip processor instance is using the related {@link DatabaseReaderLazyLoader} instance
     */
    public void test() throws Exception {
        ProjectId projectId = randomProjectIdOrDefault();
        Path geoIpConfigDir = createTempDir();
        Path geoIpTmpDir = createTempDir();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            builder(ClusterName.DEFAULT).putProjectMetadata(ProjectMetadata.builder(projectId).build()).build()
        );
        DatabaseNodeService databaseNodeService = createRegistry(
            geoIpConfigDir,
            geoIpTmpDir,
            clusterService,
            TestProjectResolvers.singleProject(projectId)
        );
        // Each loader owns its own per-version path so that retiring one cannot delete another's file. We
        // mint paths through the production helper {@link DatabaseNodeService#computeLoaderPath} so direct
        // {@code updateDatabase} calls match production semantics and any future change to the on-disk shape
        // automatically flows into this test.
        Path cityV0 = databaseNodeService.computeLoaderPath(projectId, "GeoLite2-City.mmdb");
        Path cityTestV0 = databaseNodeService.computeLoaderPath(projectId, "GeoLite2-City-Test.mmdb");
        copyDatabase("GeoLite2-City-Test.mmdb", cityV0);
        copyDatabase("GeoLite2-City-Test.mmdb", cityTestV0);
        databaseNodeService.updateDatabase(projectId, "GeoLite2-City.mmdb", "md5", cityV0);
        databaseNodeService.updateDatabase(projectId, "GeoLite2-City-Test.mmdb", "md5", cityTestV0);
        lazyLoadReaders(projectId, databaseNodeService);

        IpDataLookup lookup1 = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City.mmdb", null);
        assertNotNull(lookup1);
        IpDataLookup lookup2 = databaseNodeService.createIpDataLookup(projectId.id(), "GeoLite2-City-Test.mmdb", null);
        assertNotNull(lookup2);

        // Pre-build a cluster state once: task metadata advertises only GeoLite2-City-Test.mmdb (with the same
        // "md5" the loader has, so retrieveAndUpdateDatabase short-circuits as up-to-date), which makes
        // checkDatabases purge GeoLite2-City.mmdb via the same compute()-based cleanup path used in production.
        // Reusing the same state in the loop keeps per-iteration work comparable to a single map mutation.
        PersistentTasksCustomMetadata keepCityTestTask = DatabaseNodeServiceTests.geoIpDownloaderTask(
            projectId,
            Map.of("GeoLite2-City-Test.mmdb", new GeoIpTaskState.Metadata(0L, 0, 9, "md5", System.currentTimeMillis())),
            true
        );
        ClusterState removeCityState = DatabaseNodeServiceTests.createClusterState(
            projectId,
            keepCityTestTask,
            IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST)
        );

        final AtomicBoolean completed = new AtomicBoolean(false);
        final int numberOfDatabaseUpdates = randomIntBetween(2, 4);
        final AtomicInteger numberOfLookupRuns = new AtomicInteger();
        final int numberOfLookupThreads = randomIntBetween(16, 32);
        final Thread[] lookupThreads = new Thread[numberOfLookupThreads];
        final AtomicArray<Throwable> lookupFailures = new AtomicArray<>(numberOfLookupThreads);
        for (int i = 0; i < numberOfLookupThreads; i++) {
            final int id = i;
            lookupThreads[id] = new Thread(() -> {
                while (completed.get() == false) {
                    try {
                        Map<String, Object> result1 = lookup1.lookup("89.160.20.128");
                        if (result1 != null) {
                            assertThat(result1, not(anEmptyMap()));
                        }
                        Map<String, Object> result2 = lookup2.lookup("89.160.20.128");
                        if (result2 != null) {
                            assertThat(result2, not(anEmptyMap()));
                        }
                        numberOfLookupRuns.incrementAndGet();
                    } catch (Exception | AssertionError e) {
                        logger.error("error in lookup thread after run [" + numberOfLookupRuns.get() + "]", e);
                        lookupFailures.setOnce(id, e);
                        break;
                    }
                }
            });
        }

        final AtomicReference<Throwable> failureHolder2 = new AtomicReference<>();
        Thread updateDatabaseThread = new Thread(() -> {
            for (int i = 0; i < numberOfDatabaseUpdates; i++) {
                try {
                    DatabaseReaderLazyLoader previous1 = databaseNodeService.get(projectId, "GeoLite2-City.mmdb");
                    if (previous1 != null) {
                        Path previous1Path = previous1.getDatabasePath();
                        databaseNodeService.checkDatabases(removeCityState);
                        assertBusy(() -> {
                            // lazy loader may still be in use by an ingest thread,
                            // wait for any potential ingest thread to release the lazy loader (DatabaseReaderLazyLoader#postLookup(...)),
                            // this will do clean it up and actually deleting the underlying file
                            assertThat(Files.exists(previous1Path), is(false));
                            assertThat(previous1.current(), equalTo(-1));
                        });
                    } else {
                        Path cityVi = databaseNodeService.computeLoaderPath(projectId, "GeoLite2-City.mmdb");
                        copyDatabase("GeoLite2-City-Test.mmdb", cityVi);
                        databaseNodeService.updateDatabase(projectId, "GeoLite2-City.mmdb", "md5", cityVi);
                    }
                    DatabaseReaderLazyLoader previous2 = databaseNodeService.get(projectId, "GeoLite2-City-Test.mmdb");
                    Path cityTestVi = databaseNodeService.computeLoaderPath(projectId, "GeoLite2-City-Test.mmdb");
                    copyDatabase(i % 2 == 0 ? "GeoIP2-City-Test.mmdb" : "GeoLite2-City-Test.mmdb", cityTestVi);
                    databaseNodeService.updateDatabase(projectId, "GeoLite2-City-Test.mmdb", "md5", cityTestVi);

                    DatabaseReaderLazyLoader current1 = databaseNodeService.get(projectId, "GeoLite2-City.mmdb");
                    DatabaseReaderLazyLoader current2 = databaseNodeService.get(projectId, "GeoLite2-City-Test.mmdb");
                    assertThat(current1, not(sameInstance(previous1)));
                    assertThat(current2, not(sameInstance(previous2)));

                    // lazy load type and reader:
                    lazyLoadReaders(projectId, databaseNodeService);
                } catch (Exception | AssertionError e) {
                    logger.error("error in update databases thread after run [" + i + "]", e);
                    failureHolder2.set(e);
                    break;
                }
            }
            completed.set(true);
        });

        Arrays.stream(lookupThreads).forEach(Thread::start);
        updateDatabaseThread.start();
        Arrays.stream(lookupThreads).forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        updateDatabaseThread.join();

        lookupFailures.asList().forEach(r -> assertThat(r, nullValue()));
        assertThat(failureHolder2.get(), nullValue());
        assertThat(numberOfLookupRuns.get(), greaterThan(0));

        for (DatabaseReaderLazyLoader lazyLoader : databaseNodeService.getAllDatabases()) {
            assertThat(lazyLoader.current(), equalTo(0));
        }
        // Avoid accumulating many temp dirs while running with -Dtests.iters=X
        IOUtils.rm(geoIpConfigDir, geoIpTmpDir);
    }

    private static DatabaseNodeService createRegistry(
        Path geoIpConfigDir,
        Path geoIpTmpDir,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) throws IOException {
        GeoIpCache cache = new GeoIpCache(0);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);
        DatabaseNodeService databaseNodeService = new DatabaseNodeService(
            geoIpTmpDir,
            mock(Client.class),
            cache,
            configDatabases,
            Runnable::run,
            clusterService,
            projectResolver
        );
        databaseNodeService.initialize("nodeId", mock(ResourceWatcherService.class));
        return databaseNodeService;
    }

    private static void lazyLoadReaders(ProjectId projectId, DatabaseNodeService databaseNodeService) throws IOException {
        DatabaseReaderLazyLoader databaseReaderLazyLoader = databaseNodeService.get(projectId, "GeoLite2-City.mmdb");
        if (databaseReaderLazyLoader != null) {
            databaseReaderLazyLoader.getDatabaseType();
            databaseReaderLazyLoader.getResponse("2.125.160.216", GeoIpTestUtils::getCity);
        }
        DatabaseReaderLazyLoader databaseReaderLazyLoader1 = databaseNodeService.get(projectId, "GeoLite2-City-Test.mmdb");
        assertNotNull(databaseReaderLazyLoader1);
        databaseReaderLazyLoader1.getDatabaseType();
        databaseReaderLazyLoader1.getResponse("2.125.160.216", GeoIpTestUtils::getCity);
    }

}
