/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.geoip.AbstractGeoIpIT;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.geoip.IpLocationDownloadConsumers;
import org.elasticsearch.ingest.geoip.IpLocationTestHelper;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Integration test verifying the end-to-end ESQL consumer lifecycle for IP_LOCATION:
 * consumer registration triggered while coordinating the query, runtime sentinels while the
 * database is unavailable, and transition to real data once the database has been downloaded.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IpLocationEsqlConsumerLifecycleIT extends AbstractGeoIpIT {

    private static final String TEST_IP = "89.160.20.128";
    private static final String DATABASE_FILE = "GeoLite2-City.mmdb";
    private static final String UNAVAILABLE_SENTINEL = "_ip_location_database_unavailable_" + DATABASE_FILE;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ReindexPlugin.class,
            IngestGeoIpPlugin.class,
            IngestGeoIpSettingsPlugin.class,
            TestEncryptionServicePlugin.class,
            EsqlPluginWithEnterpriseOrTrialLicense.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        // Override the shared database path from AbstractGeoIpIT with an empty directory
        // so that no config databases are available — Scenario 1 needs the DB to be truly unavailable.
        settings.put("ingest.geoip.database_path", createTempDir().toString());
        settings.put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), false);
        return settings.build();
    }

    @After
    public void cleanUp() throws Exception {
        updateClusterSettings(Settings.builder().putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey()));
        assertBusy(
            () -> assertFalse(
                ".geoip_databases should be deleted after disabling the downloader",
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                    .get()
                    .getState()
                    .metadata()
                    .getProject(ProjectId.DEFAULT)
                    .getIndicesLookup()
                    .containsKey(IpLocationTestHelper.DATABASES_INDEX)
            )
        );
    }

    /**
     * Scenario 1: Before any DB is downloaded, an IP_LOCATION query succeeds with sentinel values
     * in KEYWORD columns and nulls elsewhere. The ESQL consumer is registered in cluster state.
     *
     * Scenario 2: After enabling the downloader and waiting for databases to propagate,
     * the same query returns real geo data.
     */
    public void testSentinelThenRealDataAfterDownload() throws Exception {
        assertNotNull("only test with fixture to have stable results", getEndpoint());

        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);

        String query = "ROW ip = \""
            + TEST_IP
            + "\" | IP_LOCATION g = ip WITH { \"properties\": [\"country_iso_code\", \"city_name\"] } | KEEP g.*";

        // --- Scenario 1: query before DB download → sentinels + consumer registered ---
        try (EsqlQueryResponse response = run(query)) {
            List<ColumnInfoImpl> columns = response.columns();
            assertEquals(2, columns.size());
            assertEquals("g.country_iso_code", columns.get(0).name());
            assertEquals("g.city_name", columns.get(1).name());

            List<List<Object>> values = getValuesList(response);
            assertEquals(1, values.size());
            assertEquals(UNAVAILABLE_SENTINEL, values.getFirst().getFirst());
            assertEquals(UNAVAILABLE_SENTINEL, values.getFirst().get(1));
        }

        assertEsqlConsumerRegistered();

        // --- Scenario 2: enable download, wait for DB propagation, query returns real data ---
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        IpLocationTestHelper.awaitAllDatabasesAvailable(internalCluster(), IpLocationConsumer.ESQL);

        try (EsqlQueryResponse response = runOnDataNode(query)) {
            List<List<Object>> values = getValuesList(response);
            assertEquals(1, values.size());
            assertEquals("SE", values.getFirst().getFirst());
            assertEquals("Linköping", values.getFirst().get(1));
        }
    }

    /**
     * Verifies that a query requesting non-KEYWORD fields (like location, accuracy_radius)
     * also gets nulls (not sentinels) for those fields when the DB is unavailable.
     */
    public void testNonKeywordFieldsAreNullWhenUnavailable() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);

        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNodes(1);

        String query = "ROW ip = \""
            + TEST_IP
            + "\" | IP_LOCATION g = ip WITH { \"properties\": [\"country_iso_code\", \"location\", \"accuracy_radius\"] } | KEEP g.*";

        try (EsqlQueryResponse response = run(query)) {
            List<List<Object>> values = getValuesList(response);
            assertEquals(1, values.size());
            assertEquals(UNAVAILABLE_SENTINEL, values.getFirst().getFirst());
            assertNull(values.getFirst().get(1));
            assertNull(values.getFirst().get(2));
        }
    }

    /**
     * Verifies that a master-only node can coordinate an IP_LOCATION query after databases
     * have been downloaded. This catches the case where {@code isRelevantForNode} excludes
     * master-only nodes from database staging.
     */
    public void testMasterOnlyCoordinatorReturnsRealData() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);

        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);

        // Trigger ESQL consumer registration and enable download
        String query = "ROW ip = \""
            + TEST_IP
            + "\" | IP_LOCATION g = ip WITH { \"properties\": [\"country_iso_code\", \"city_name\"] } | KEEP g.*";
        run(query).close();

        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));
        IpLocationTestHelper.awaitAllDatabasesAvailable(internalCluster(), IpLocationConsumer.ESQL);

        // Query explicitly through the master-only node as coordinator
        try (
            EsqlQueryResponse response = internalCluster().masterClient()
                .execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query))
                .actionGet(TimeValue.timeValueSeconds(30))
        ) {
            List<List<Object>> values = getValuesList(response);
            assertEquals(1, values.size());
            assertEquals("SE", values.getFirst().getFirst());
            assertEquals("Linköping", values.getFirst().get(1));
        }
    }

    private EsqlQueryResponse run(String esqlCommands) {
        return client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(esqlCommands)).actionGet(TimeValue.timeValueSeconds(30));
    }

    private EsqlQueryResponse runOnDataNode(String esqlCommands) {
        return internalCluster().dataNodeClient()
            .execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(esqlCommands))
            .actionGet(TimeValue.timeValueSeconds(30));
    }

    private void assertEsqlConsumerRegistered() throws Exception {
        assertBusy(() -> {
            IpLocationDownloadConsumers consumers = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .metadata()
                .getProject(ProjectId.DEFAULT)
                .custom(IpLocationDownloadConsumers.TYPE, IpLocationDownloadConsumers.EMPTY);
            assertTrue("ESQL consumer should be registered after first IP_LOCATION query", consumers.contains(IpLocationConsumer.ESQL));
        });
    }
}
