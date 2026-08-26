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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
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

    /**
     * The set of databases served by {@link fixture.geoip.GeoIpHttpFixture}.
     */
    public static final String[] FIXTURE_DATABASES = new String[] {
        "GeoLite2-City.mmdb",
        "GeoLite2-Country.mmdb",
        "GeoLite2-ASN.mmdb",
        "MyCustomGeoLite2-City.mmdb" };

    private IpLocationTestHelper() {}

    /**
     * Overrides the default enterprise download endpoints for testing.
     */
    public static void setEnterpriseEndpoints(String endpoint) {
        EnterpriseGeoIpDownloader.DEFAULT_MAXMIND_ENDPOINT = endpoint;
        EnterpriseGeoIpDownloader.DEFAULT_IPINFO_ENDPOINT = endpoint;
    }

    /**
     * Requests IP database downloads for the {@link IpLocationConsumer#INGEST INGEST} consumer.
     * The request is internally routed to the master node via a transport action, so a single call suffices.
     */
    public static void requestDownloads(InternalTestCluster cluster, String projectId) {
        requestDownloads(cluster, projectId, IpLocationConsumer.INGEST);
    }

    /**
     * Requests IP database downloads for the specified consumer type.
     * The {@link IpLocationService} implementation internally routes the request to the master node via a
     * transport action that updates cluster state metadata, so a single call from any node suffices.
     */
    public static void requestDownloads(InternalTestCluster cluster, String projectId, IpLocationConsumer consumer) {
        cluster.getInstance(IpLocationService.class).requestDownloads(projectId, consumer);
    }

    /**
     * Cancels a previous IP database download request for the specified consumer type.
     * Like {@link #requestDownloads}, the cancellation is internally routed to the master node via a
     * transport action and updates cluster state metadata, so a single call from any node suffices.
     */
    public static void cancelDownloadRequest(InternalTestCluster cluster, String projectId, IpLocationConsumer consumer) {
        cluster.getInstance(IpLocationService.class).cancelDownloadRequest(projectId, consumer);
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
        assertDatabaseAvailable(service, projectId, databaseFile, ip, expectedField, expectedValue);
    }

    /**
     * Asserts that the given database is available and that a lookup for the
     * given IP returns the expected value for the specified field.
     */
    public static void assertDatabaseAvailable(
        IpLocationService service,
        String projectId,
        String databaseFile,
        String ip,
        String expectedField,
        Object expectedValue
    ) throws IOException {
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
     * Waits until all nodes relevant for {@code consumer} have downloaded the fixture databases
     * <strong>and</strong> the databases are functionally usable (a lookup returns non-null).
     */
    public static void awaitAllDatabasesAvailable(InternalTestCluster cluster, IpLocationConsumer consumer) throws Exception {
        awaitAllDatabasesDownloaded(cluster.client(), consumer);
        assertBusy(() -> {
            for (String nodeName : cluster.getNodeNames()) {
                DiscoveryNode node = cluster.clusterService(nodeName).localNode();
                if (IpLocationDownloadConsumers.isRelevantForNode(consumer, node) == false) {
                    continue;
                }
                IpLocationService service = cluster.getInstance(IpLocationService.class, nodeName);
                for (String db : FIXTURE_DATABASES) {
                    IpDataLookup lookup = service.createIpDataLookup(ProjectId.DEFAULT.id(), db, null);
                    assertNotNull("database [" + db + "] should be available for lookup on node [" + nodeName + "]", lookup);
                }
            }
        });
    }

    private static void awaitAllDatabasesDownloaded(Client client, IpLocationConsumer consumer) throws Exception {
        assertBusy(() -> {
            GeoIpStatsAction.Response response = client.execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));

            List<GeoIpStatsAction.NodeResponse> relevant = response.getNodes()
                .stream()
                .filter(r -> IpLocationDownloadConsumers.isRelevantForNode(consumer, r.getNode()))
                .toList();
            assertThat("expected at least one node relevant for consumer [" + consumer + "]", relevant, not(empty()));

            for (GeoIpStatsAction.NodeResponse nodeResponse : relevant) {
                assertThat(
                    "node [" + nodeResponse.getNode().getName() + "] (relevant for " + consumer + ") should have all downloaded databases",
                    nodeResponse.getDatabases(),
                    containsInAnyOrder(FIXTURE_DATABASES)
                );
            }
        });
    }

    /**
     * Waits until all nodes report no downloaded databases and no mmdb files in the temp directory.
     * Call this after disabling the downloader to ensure async file cleanup has finished before
     * the next test starts.
     */
    public static void awaitNoDatabases(InternalTestCluster cluster) throws Exception {
        assertBusy(() -> {
            GeoIpStatsAction.Response response = cluster.client()
                .execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request())
                .actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                assertThat(nodeResponse.getDatabases(), empty());
                // ignore the README/LICENSE files with .txt extension
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
