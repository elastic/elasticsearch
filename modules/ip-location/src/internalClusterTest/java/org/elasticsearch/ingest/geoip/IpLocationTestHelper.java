/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.safeSleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Utility methods for ip-location internal cluster tests that use the
 * {@link IpLocationService} API directly, without ingest pipelines.
 * Also provides access to package-private constants needed by tests in other modules.
 */
public final class IpLocationTestHelper {

    private static final Logger logger = LogManager.getLogger(IpLocationTestHelper.class);

    public static final String DATABASES_INDEX = GeoIpDownloader.DATABASES_INDEX;

    private IpLocationTestHelper() {}

    /**
     * Overrides the default enterprise download endpoints for testing.
     */
    public static void setEnterpriseEndpoints(String endpoint) {
        EnterpriseGeoIpDownloader.DEFAULT_MAXMIND_ENDPOINT = endpoint;
        EnterpriseGeoIpDownloader.DEFAULT_IPINFO_ENDPOINT = endpoint;
    }

    /**
     * Calls {@link IpLocationService#requestDownloads} on every node in the cluster.
     * Each node has its own {@code downloadRequested} flag, so all must be set for
     * {@code checkDatabases()} to run cluster-wide — mirroring what
     * {@code IngestIpLocationPlugin.clusterChanged()} does per-node.
     */
    public static void requestDownloads(InternalTestCluster cluster, String projectId) {
        for (IpLocationService service : cluster.getInstances(IpLocationService.class)) {
            service.requestDownloads(projectId);
        }
    }

    /**
     * Asserts that the given database is available and that a lookup for the
     * given IP returns the expected value for the specified field.
     */
    public static void assertDatabaseAvailable(
        InternalTestCluster cluster,
        String projectId,
        String databaseFile,
        String ip,
        String expectedField,
        Object expectedValue
    ) throws IOException {
        IpLocationService service = cluster.getAnyMasterNodeInstance(IpLocationService.class);
        IpDataLookup lookup = service.createIpDataLookup(projectId, databaseFile, null);
        assertNotNull("database [" + databaseFile + "] should be available", lookup);
        Map<String, Object> result = lookup.lookup(ip);
        assertNotNull("lookup for [" + ip + "] in [" + databaseFile + "] should return data", result);
        assertThat("field [" + expectedField + "] in lookup result", result.get(expectedField), equalTo(expectedValue));
    }

    /**
     * Asserts that the given database is not currently available on the cluster
     * (i.e. {@code createIpDataLookup} returns {@code null}).
     */
    public static void assertDatabaseUnavailable(InternalTestCluster cluster, String projectId, String databaseFile) {
        IpLocationService service = cluster.getAnyMasterNodeInstance(IpLocationService.class);
        IpDataLookup lookup = service.createIpDataLookup(projectId, databaseFile, null);
        assertNull("database [" + databaseFile + "] should not be available", lookup);
    }

    /**
     * Copies the default GeoLite2 databases into each node's {@code configDir/ingest-geoip} directory
     * and waits until all nodes report them as available config databases.
     */
    public static void setupDatabasesInConfigDirectory(InternalTestCluster cluster) throws Exception {
        StreamSupport.stream(cluster.getInstances(Environment.class).spliterator(), false)
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
            GeoIpStatsAction.Response response = cluster.client()
                .execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request())
                .actionGet();
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

    /**
     * Removes the {@code configDir/ingest-geoip} directory from all nodes and waits
     * until all nodes report no config databases. Handles Windows filesystem retry.
     */
    public static void deleteDatabasesInConfigDirectory(InternalTestCluster cluster) throws Exception {
        StreamSupport.stream(cluster.getInstances(Environment.class).spliterator(), false)
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
            GeoIpStatsAction.Response response = cluster.client()
                .execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request())
                .actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getConfigDatabases(), empty());
            }
        });
    }
}
