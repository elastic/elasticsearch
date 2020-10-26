/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.LAST_UPDATED_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.monitoring.exporter.http.ClusterAlertHttpResource.CLUSTER_ALERT_VERSION_PARAMETERS;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.FILTER_PATH_RESOURCE_VERSION;
import static org.elasticsearch.xpack.monitoring.exporter.http.WatcherExistsHttpResource.WATCHER_CHECK_PARAMETERS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class TransportMonitoringMigrateAlertsActionTests extends MonitoringIntegTestCase {

    private final List<String> clusterAlertBlacklist = new ArrayList<>();
    private MockWebServer webServer;

    private MockWebServer createMockWebServer() throws IOException {
        MockWebServer server = new MockWebServer();
        server.start();
        return server;
    }

    @Before
    public void startWebServer() throws IOException {
        webServer = createMockWebServer();
    }

    @After
    public void stopWebServer() {
        if (webServer != null) {
            webServer.close();
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            // Parent conf
            .put(super.nodeSettings(nodeOrdinal))

            // Disable monitoring
            .put("xpack.monitoring.collection.enabled", false)
            .put("xpack.monitoring.collection.interval", "1s")

            // X-Pack configuration
            .put("xpack.license.self_generated.type", "trial")
            .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
            .build();
    }

    private void stopMonitoring() {
        // Clean up any transient settings we have added
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .putNull(MonitoringService.ENABLED.getKey())
            .putNull("xpack.monitoring.elasticsearch.collection.enabled")
            .putNull("xpack.monitoring.exporters._local.type")
            .putNull("xpack.monitoring.exporters._local.enabled")
            .putNull("xpack.monitoring.exporters._local.cluster_alerts.management.enabled")
            .putNull("xpack.monitoring.exporters.remoteCluster.type")
            .putNull("xpack.monitoring.exporters.remoteCluster.enabled")
            .putNull("xpack.monitoring.exporters.remoteCluster.host")
            .putNull("xpack.monitoring.exporters.remoteCluster.cluster_alerts.management.enabled")
        ));
        // Make sure to clean up the migration setting if it is set
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .putNull(Monitoring.MIGRATION_DECOMMISSION_ALERTS.getKey())
        ));
    }

    @Test
    public void testLocalAlertsRemoval() throws Exception {
        try {
            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", true);

            // enable local exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // ensure resources exist
            assertBusy(() -> {
                assertThat(indexExists(".monitoring-*"), is(true));
                ensureYellowAndNoInitializingShards(".monitoring-*");
                checkMonitoringTemplates();
                assertWatchesExist(true);
            });

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check response
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("_local"));
            assertThat(localExporterResult.getType(), is(LocalExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(true));
            assertThat(localExporterResult.getReason(), nullValue());

            // ensure no watches
            assertWatchesExist(false);
        } finally {
            stopMonitoring();
        }
    }

    @Test
    public void testDisabledLocalExporterAlertsRemoval() throws Exception {
        try {
            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", true);

            // enable local exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // ensure resources exist
            ensureInitialLocalResources();

            // new disable local exporter
            final Settings.Builder disableSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", false)
                .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", true);
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(disableSettings));

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check response
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("_local"));
            assertThat(localExporterResult.getType(), is(LocalExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(true));
            assertThat(localExporterResult.getReason(), nullValue());

            // ensure no watches
            assertWatchesExist(false);
        } finally {
            stopMonitoring();
        }
    }

    @Test
    public void testLocalExporterWithAlertingDisabled() throws Exception {
        try {
            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", true);

            // enable local exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // ensure resources exist
            ensureInitialLocalResources();

            // new disable local exporter's cluster alerts
            final Settings.Builder disableSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", false);
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(disableSettings));

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check response
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("_local"));
            assertThat(localExporterResult.getType(), is(LocalExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(false));
            assertThat(localExporterResult.getReason(), notNullValue());
            assertThat(localExporterResult.getReason().getMessage(), is("cannot manage cluster alerts because alerting is disabled"));
        } finally {
            stopMonitoring();
        }
    }

    @Test
    public void testRemoteAlertsRemoval() throws Exception {
        try {
            // prepare mock web server
            enqueueGetClusterVersionResponse(Version.CURRENT);
            enqueueSetupResponses(webServer, false, true, false, true, true, false);
            enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                // Make sure to not collect ES stats in background. Our web server expects requests in a particular order.
                .put("xpack.monitoring.elasticsearch.collection.enabled", false)
                .put("xpack.monitoring.exporters.remoteCluster.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters.remoteCluster.enabled", true)
                .put("xpack.monitoring.exporters.remoteCluster.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters.remoteCluster.cluster_alerts.management.enabled", true);

            // enable http exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // send a manual monitoring bulk request to kickstart the monitoring bootstrap for the remote exporter
            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.ES, "test", "test-id", System.currentTimeMillis(), 0L, new BytesArray("{}"), XContentType.JSON);
            MonitoringBulkRequest bulkRequest = new MonitoringBulkRequest().add(doc);
            client().execute(MonitoringBulkAction.INSTANCE, bulkRequest).actionGet();

            // ensure resources exist and bulk successful
            assertMonitorResources(webServer, false, true, false, true, true, false);
            assertBulk(webServer, 1);

            // enqueue delete request expectations for alerts
            clusterAlertBlacklist.addAll(Arrays.asList(ClusterAlertsUtil.WATCH_IDS)); // Set the expectations that watches are deleted
            enqueueWatcherResponses(webServer, true, true, true);

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check response
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("remoteCluster"));
            assertThat(localExporterResult.getType(), is(HttpExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(true));
            assertThat(localExporterResult.getReason(), nullValue());

            // ensure no watches
            assertMonitorWatches(webServer, true, true, true, null, null);
        } finally {
            stopMonitoring();
            clusterAlertBlacklist.clear();
            webServer.clearRequests();
        }
    }

    @Test
    public void testDisabledRemoteAlertsRemoval() throws Exception {
        try {
            // prepare mock web server
            enqueueGetClusterVersionResponse(Version.CURRENT);
            enqueueSetupResponses(webServer, false, true, false, true, true, false);
            enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                // Make sure to not collect ES stats in background. Our web server expects requests in a particular order.
                .put("xpack.monitoring.elasticsearch.collection.enabled", false)
                .put("xpack.monitoring.exporters.remoteCluster.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters.remoteCluster.enabled", true)
                .put("xpack.monitoring.exporters.remoteCluster.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters.remoteCluster.cluster_alerts.management.enabled", true);

            // enable http exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // send a manual monitoring bulk request to kickstart the monitoring bootstrap for the remote exporter
            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.ES, "test", "test-id", System.currentTimeMillis(), 0L, new BytesArray("{}"), XContentType.JSON);
            MonitoringBulkRequest bulkRequest = new MonitoringBulkRequest().add(doc);
            client().execute(MonitoringBulkAction.INSTANCE, bulkRequest).actionGet();

            // ensure resources exist and bulk successful
            assertMonitorResources(webServer, false, true, false, true, true, false);
            assertBulk(webServer, 1);

            // start monitoring service
            final Settings.Builder disabledExporterSettings = Settings.builder()
                .put("xpack.monitoring.exporters.remoteCluster.enabled", false);

            // disable http exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(disabledExporterSettings));

            // enqueue delete request expectations for alerts
            clusterAlertBlacklist.addAll(Arrays.asList(ClusterAlertsUtil.WATCH_IDS)); // Set the expectations that watches are deleted
            enqueueWatcherResponses(webServer, true, true, true);

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check that the http exporter was tapped to remove watches
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("remoteCluster"));
            assertThat(localExporterResult.getType(), is(HttpExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(true));
            assertThat(localExporterResult.getReason(), nullValue());

            // ensure no watches
            assertMonitorWatches(webServer, true, true, true, null, null);
        } finally {
            stopMonitoring();
            clusterAlertBlacklist.clear();
            webServer.clearRequests();
        }
    }

    @Test
    public void testRemoteAlertsRemovalWhenOriginalMonitoringClusterIsGone() throws Exception {
        try {
            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                // Make sure to not collect ES stats in background. Our web server expects requests in a particular order.
                .put("xpack.monitoring.elasticsearch.collection.enabled", false)
                .put("xpack.monitoring.exporters.remoteCluster.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters.remoteCluster.enabled", false)
                .put("xpack.monitoring.exporters.remoteCluster.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters.remoteCluster.cluster_alerts.management.enabled", true);

            // create a disabled http exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check that migration failed due to monitoring cluster not responding
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("remoteCluster"));
            assertThat(localExporterResult.getType(), is(HttpExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(false));
            // this might be a messier exception in practice like connection refused, but hey, reproduceability
            assertThat(localExporterResult.getReason().getMessage(), is("Connection is closed"));
        } finally {
            stopMonitoring();
            clusterAlertBlacklist.clear();
            webServer.clearRequests();
        }
    }

    @Test
    public void testRemoteAlertsRemovalFailure() throws Exception {
        try {
            // prepare mock web server
            enqueueGetClusterVersionResponse(Version.CURRENT);
            enqueueSetupResponses(webServer, false, true, false, true, true, false);
            enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

            // start monitoring service
            final Settings.Builder exporterSettings = Settings.builder()
                .put(MonitoringService.ENABLED.getKey(), true)
                // Make sure to not collect ES stats in background. Our web server expects requests in a particular order.
                .put("xpack.monitoring.elasticsearch.collection.enabled", false)
                .put("xpack.monitoring.exporters.remoteCluster.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters.remoteCluster.enabled", true)
                .put("xpack.monitoring.exporters.remoteCluster.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters.remoteCluster.cluster_alerts.management.enabled", true);

            // enable http exporter
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));

            // send a manual monitoring bulk request to kickstart the monitoring bootstrap for the remote exporter
            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.ES, "test", "test-id", System.currentTimeMillis(), 0L, new BytesArray("{}"), XContentType.JSON);
            client().execute(MonitoringBulkAction.INSTANCE, new MonitoringBulkRequest().add(doc)).actionGet();

            // ensure resources exist and bulk successful
            assertMonitorResources(webServer, false, true, false, true, true, false);
            assertBulk(webServer, 1);

            // enqueue a watcher available response, but then a failure to delete response
            enqueueResponse(webServer, 200, "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":true}}}");
            enqueueResponse(webServer, 500, "{\"error\":{}}");

            // call migration api
            MonitoringMigrateAlertsResponse response = client().execute(MonitoringMigrateAlertsAction.INSTANCE,
                new MonitoringMigrateAlertsRequest()).actionGet();

            // check response
            assertThat(response.getExporters().size(), is(1));
            MonitoringMigrateAlertsResponse.ExporterMigrationResult localExporterResult = response.getExporters().get(0);
            assertThat(localExporterResult.getName(), is("remoteCluster"));
            assertThat(localExporterResult.getType(), is(HttpExporter.TYPE));
            assertThat(localExporterResult.isMigrationComplete(), is(false));
            assertThat(localExporterResult.getReason().getMessage(), startsWith("method [DELETE], host ["));
            assertThat(localExporterResult.getReason().getMessage(), endsWith("status line [HTTP/1.1 500 Internal Server Error]\n{\"error\":{}}"));

        } finally {
            stopMonitoring();
            clusterAlertBlacklist.clear();
            webServer.clearRequests();
        }
    }

    private void ensureInitialLocalResources() throws Exception {
        assertBusy(() -> {
            assertThat(indexExists(".monitoring-*"), is(true));
            ensureYellowAndNoInitializingShards(".monitoring-*");
            checkMonitoringTemplates();
            assertWatchesExist(true);
        }, 20, TimeUnit.SECONDS); // Watcher can be slow to allocate all watches required
    }

    /**
     * Checks that the monitoring templates have been created by the local exporter
     */
    private void checkMonitoringTemplates() {
        final Set<String> templates = new HashSet<>();
        templates.add(".monitoring-alerts-7");
        templates.add(".monitoring-es");
        templates.add(".monitoring-kibana");
        templates.add(".monitoring-logstash");
        templates.add(".monitoring-beats");

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(".monitoring-*").get();
        Set<String> actualTemplates = response.getIndexTemplates().stream().map(IndexTemplateMetadata::getName).collect(Collectors.toSet());
        assertEquals(templates, actualTemplates);
    }

    private void assertWatchesExist(boolean exist) {
        // Check if watches index exists
        if (client().admin().indices().prepareGetIndex().addIndices(".watches").get().getIndices().length == 0) {
            fail("Expected [.watches] index with cluster alerts present, but no [.watches] index was found");
        }

        Arrays.stream(ClusterAlertsUtil.WATCH_IDS)
            .map(n -> ClusterAlertsUtil.createUniqueWatchId(clusterService(), n))
            .map(watch -> client().execute(GetWatchAction.INSTANCE, new GetWatchRequest(watch)).actionGet())
            .filter(r -> r.isFound() != exist)
            .findAny()
            .ifPresent(r -> fail((exist ? "missing" : "found") + " watch [" + r.getId() + "]"));
    }

    protected List<String> monitoringTemplateNames() {
        return Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
            .map(MonitoringTemplateUtils::templateName)
            .collect(Collectors.toList());
    }

    // this can be removed in 7.0
    protected List<String> monitoringTemplateNamesWithOldTemplates() {
        final List<String> expectedTemplateNames = monitoringTemplateNames();

        expectedTemplateNames.addAll(
            Arrays.stream(MonitoringTemplateUtils.OLD_TEMPLATE_IDS)
                .map(MonitoringTemplateUtils::oldTemplateName)
                .collect(Collectors.toList()));

        return expectedTemplateNames;
    }

    private List<String> monitoringTemplateNames(final boolean includeOldTemplates) {
        return includeOldTemplates ? monitoringTemplateNamesWithOldTemplates() : monitoringTemplateNames();
    }

    private void enqueueGetClusterVersionResponse(Version v) throws IOException {
        enqueueGetClusterVersionResponse(webServer, v);
    }

    private void enqueueGetClusterVersionResponse(MockWebServer mockWebServer, Version v) throws IOException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(
            BytesReference.bytes(jsonBuilder().startObject().startObject("version")
                .field("number", v.toString()).endObject().endObject()).utf8ToString()));
    }

    private void enqueueSetupResponses(final MockWebServer webServer,
                                       final boolean templatesAlreadyExists, final boolean includeOldTemplates,
                                       final boolean pipelineAlreadyExists,
                                       final boolean remoteClusterAllowsWatcher, final boolean currentLicenseAllowsWatcher,
                                       final boolean watcherAlreadyExists) throws IOException {
        enqueueTemplateResponses(webServer, templatesAlreadyExists, includeOldTemplates);
        enqueuePipelineResponses(webServer, pipelineAlreadyExists);
        enqueueWatcherResponses(webServer, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
    }

    private void enqueueTemplateResponses(final MockWebServer webServer,
                                          final boolean alreadyExists, final boolean includeOldTemplates)
        throws IOException {
        if (alreadyExists) {
            enqueueTemplateResponsesExistsAlready(webServer, includeOldTemplates);
        } else {
            enqueueTemplateResponsesDoesNotExistYet(webServer, includeOldTemplates);
        }
    }

    private void enqueueTemplateResponsesDoesNotExistYet(final MockWebServer webServer,
                                                         final boolean includeOldTemplates)
        throws IOException {
        enqueueVersionedResourceResponsesDoesNotExistYet(monitoringTemplateNames(includeOldTemplates), webServer);
    }

    private void enqueueTemplateResponsesExistsAlready(final MockWebServer webServer,
                                                       final boolean includeOldTemplates)
        throws IOException {
        enqueueVersionedResourceResponsesExistsAlready(monitoringTemplateNames(includeOldTemplates), webServer);
    }

    private void enqueuePipelineResponses(final MockWebServer webServer, final boolean alreadyExists) throws IOException {
        if (alreadyExists) {
            enqueuePipelineResponsesExistsAlready(webServer);
        } else {
            enqueuePipelineResponsesDoesNotExistYet(webServer);
        }
    }

    private void enqueuePipelineResponsesDoesNotExistYet(final MockWebServer webServer) throws IOException {
        enqueueVersionedResourceResponsesDoesNotExistYet(monitoringPipelineNames(), webServer);
    }

    private void enqueuePipelineResponsesExistsAlready(final MockWebServer webServer) throws IOException {
        enqueueVersionedResourceResponsesExistsAlready(monitoringPipelineNames(), webServer);
    }

    private void enqueueVersionedResourceResponsesDoesNotExistYet(final List<String> names, final MockWebServer webServer)
        throws IOException {
        for (String resource : names) {
            if (randomBoolean()) {
                enqueueResponse(webServer, 404, "[" + resource + "] does not exist");
            } else if (randomBoolean()) {
                final int version = LAST_UPDATED_VERSION - randomIntBetween(1, 1000000);

                // it DOES exist, but it's an older version
                enqueueResponse(webServer, 200, "{\"" + resource + "\":{\"version\":" + version + "}}");
            } else {
                // no version specified
                enqueueResponse(webServer, 200, "{\"" + resource + "\":{}}");
            }
            enqueueResponse(webServer, 201, "[" + resource + "] created");
        }
    }

    private void enqueueVersionedResourceResponsesExistsAlready(final List<String> names, final MockWebServer webServer)
        throws IOException {
        for (String resource : names) {
            if (randomBoolean()) {
                final int newerVersion = randomFrom(Version.CURRENT.id, LAST_UPDATED_VERSION) + randomIntBetween(1, 1000000);

                // it's a NEWER resource (template / pipeline)
                enqueueResponse(webServer, 200, "{\"" + resource + "\":{\"version\":" + newerVersion + "}}");
            } else {
                // we already put it
                enqueueResponse(webServer, 200, "{\"" + resource + "\":{\"version\":" + LAST_UPDATED_VERSION + "}}");
            }
        }
    }

    private void enqueueWatcherResponses(final MockWebServer webServer,
                                         final boolean remoteClusterAllowsWatcher, final boolean currentLicenseAllowsWatcher,
                                         final boolean alreadyExists) throws IOException {
        // if the remote cluster doesn't allow watcher, then we only check for it and we're done
        if (remoteClusterAllowsWatcher) {
            // X-Pack exists and Watcher can be used
            enqueueResponse(webServer, 200, "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":true}}}");

            // if we have an active license that's not Basic, then we should add watches
            if (currentLicenseAllowsWatcher) {
                if (alreadyExists) {
                    enqueueClusterAlertResponsesExistsAlready(webServer);
                } else {
                    enqueueClusterAlertResponsesDoesNotExistYet(webServer);
                }
            } else {
                // otherwise we need to delete them from the remote cluster
                enqueueDeleteClusterAlertResponses(webServer);
            }
        } else {
            // X-Pack exists but Watcher just cannot be used
            if (randomBoolean()) {
                final String responseBody = randomFrom(
                    "{\"features\":{\"watcher\":{\"available\":false,\"enabled\":true}}}",
                    "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":false}}}",
                    "{}"
                );

                enqueueResponse(webServer, 200, responseBody);
            } else {
                // X-Pack is not installed
                enqueueResponse(webServer, 404, "{}");
            }
        }
    }

    private void enqueueClusterAlertResponsesDoesNotExistYet(final MockWebServer webServer)
        throws IOException {
        for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
            if (clusterAlertBlacklist.contains(watchId)) {
                enqueueDeleteClusterAlertResponse(webServer, watchId);
            } else {
                if (randomBoolean()) {
                    enqueueResponse(webServer, 404, "watch [" + watchId + "] does not exist");
                } else if (randomBoolean()) {
                    final int version = ClusterAlertsUtil.LAST_UPDATED_VERSION - randomIntBetween(1, 1000000);

                    // it DOES exist, but it's an older version
                    enqueueResponse(webServer, 200, "{\"metadata\":{\"xpack\":{\"version_created\":" + version + "}}}");
                } else {
                    // no version specified
                    enqueueResponse(webServer, 200, "{\"metadata\":{\"xpack\":{}}}");
                }
                enqueueResponse(webServer, 201, "[" + watchId + "] created");
            }
        }
    }

    private void enqueueClusterAlertResponsesExistsAlready(final MockWebServer webServer) throws IOException {
        for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
            if (clusterAlertBlacklist.contains(watchId)) {
                enqueueDeleteClusterAlertResponse(webServer, watchId);
            } else {
                final int existsVersion;

                if (randomBoolean()) {
                    // it's a NEWER cluster alert
                    existsVersion = randomFrom(Version.CURRENT.id, ClusterAlertsUtil.LAST_UPDATED_VERSION) + randomIntBetween(1, 1000000);
                } else {
                    // we already put it
                    existsVersion = ClusterAlertsUtil.LAST_UPDATED_VERSION;
                }

                enqueueResponse(webServer, 200, "{\"metadata\":{\"xpack\":{\"version_created\":" + existsVersion + "}}}");
            }
        }
    }

    private void enqueueDeleteClusterAlertResponses(final MockWebServer webServer) throws IOException {
        for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
            enqueueDeleteClusterAlertResponse(webServer, watchId);
        }
    }

    private void enqueueDeleteClusterAlertResponse(final MockWebServer webServer, final String watchId) throws IOException {
        if (randomBoolean()) {
            enqueueResponse(webServer, 404, "watch [" + watchId + "] did not exist");
        } else {
            enqueueResponse(webServer, 200, "watch [" + watchId + "] deleted");
        }
    }

    private void enqueueResponse(int responseCode, String body) throws IOException {
        enqueueResponse(webServer, responseCode, body);
    }

    private void enqueueResponse(MockWebServer mockWebServer, int responseCode, String body) throws IOException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));
    }

    private String basePathToAssertablePrefix(@Nullable String basePath) {
        if (basePath == null) {
            return "";
        }
        basePath = basePath.startsWith("/")? basePath : "/" + basePath;
        return basePath;
    }

    private List<Tuple<String, String>> monitoringTemplates(final boolean includeOldTemplates) {
        return includeOldTemplates ? monitoringTemplatesWithOldTemplates() : monitoringTemplates();
    }

    // this can be removed in 7.0
    private List<Tuple<String, String>> monitoringTemplatesWithOldTemplates() {
        final List<Tuple<String, String>> expectedTemplates = monitoringTemplates();

        expectedTemplates.addAll(
            Arrays.stream(MonitoringTemplateUtils.OLD_TEMPLATE_IDS)
                .map(id -> new Tuple<>(MonitoringTemplateUtils.oldTemplateName(id), MonitoringTemplateUtils.createEmptyTemplate(id)))
                .collect(Collectors.toList()));

        return expectedTemplates;
    }

    private String resourceVersionQueryString() {
        return "filter_path=" + FILTER_PATH_RESOURCE_VERSION;
    }

    private String watcherCheckQueryString() {
        return "filter_path=" + WATCHER_CHECK_PARAMETERS.get("filter_path");
    }

    private String resourceClusterAlertQueryString() {
        return "filter_path=" + CLUSTER_ALERT_VERSION_PARAMETERS.get("filter_path");
    }

    private String bulkQueryString() {
        final String pipelineName = MonitoringTemplateUtils.pipelineName(TEMPLATE_VERSION);

        return "pipeline=" + pipelineName + "&filter_path=" + "errors,items.*.error";
    }

    private String getExternalTemplateRepresentation(String internalRepresentation) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, internalRepresentation)) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            IndexTemplateMetadata.Builder.removeType(IndexTemplateMetadata.Builder.fromXContent(parser, ""), builder);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private void assertBulkRequest(String requestBody, int numberOfActions) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest()
            .add(new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(numberOfActions));
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests()) {
            assertThat(actionRequest, instanceOf(IndexRequest.class));
        }
    }

    private void assertMonitorVersion(final MockWebServer webServer) throws Exception {
        assertMonitorVersion(webServer, null, null);
    }

    private void assertMonitorVersion(final MockWebServer webServer, @Nullable final Map<String, String[]> customHeaders,
                                      @Nullable final String basePath) throws Exception {
        final MockRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("GET"));
        final String pathPrefix = basePathToAssertablePrefix(basePath);
        if (Strings.isEmpty(pathPrefix) == false) {
            assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/"));
        }
        assertThat(request.getUri().getQuery(), equalTo("filter_path=version.number"));
        assertHeaders(request, customHeaders);
    }

    private void assertMonitorResources(final MockWebServer webServer,
                                        final boolean templateAlreadyExists, final boolean includeOldTemplates,
                                        final boolean pipelineAlreadyExists,
                                        final boolean remoteClusterAllowsWatcher, final boolean currentLicenseAllowsWatcher,
                                        final boolean watcherAlreadyExists) throws Exception {
        assertMonitorResources(webServer, templateAlreadyExists, includeOldTemplates, pipelineAlreadyExists,
            remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists,
            null, null);
    }

    private void assertMonitorResources(final MockWebServer webServer,
                                        final boolean templateAlreadyExists, final boolean includeOldTemplates,
                                        final boolean pipelineAlreadyExists,
                                        final boolean remoteClusterAllowsWatcher, final boolean currentLicenseAllowsWatcher,
                                        final boolean watcherAlreadyExists,
                                        @Nullable final Map<String, String[]> customHeaders,
                                        @Nullable final String basePath) throws Exception {
        assertMonitorVersion(webServer, customHeaders, basePath);
        assertMonitorTemplates(webServer, templateAlreadyExists, includeOldTemplates, customHeaders, basePath);
        assertMonitorPipelines(webServer, pipelineAlreadyExists, customHeaders, basePath);
        assertMonitorWatches(webServer, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists,
            customHeaders, basePath);
    }

    private void assertMonitorTemplates(final MockWebServer webServer,
                                        final boolean alreadyExists,
                                        final boolean includeOldTemplates,
                                        @Nullable final Map<String, String[]> customHeaders,
                                        @Nullable final String basePath) throws Exception {
        final List<Tuple<String, String>> templates = monitoringTemplates(includeOldTemplates);

        assertMonitorVersionResource(webServer, alreadyExists, "/_template/", templates, customHeaders, basePath);
    }

    private void assertMonitorPipelines(final MockWebServer webServer,
                                        final boolean alreadyExists,
                                        @Nullable final Map<String, String[]> customHeaders,
                                        @Nullable final String basePath) throws Exception {
        assertMonitorVersionResource(webServer, alreadyExists, "/_ingest/pipeline/", monitoringPipelines(),
            customHeaders, basePath);
    }

    private void assertMonitorVersionResource(final MockWebServer webServer, final boolean alreadyExists,
                                              final String resourcePrefix, final List<Tuple<String, String>> resources,
                                              @Nullable final Map<String, String[]> customHeaders,
                                              @Nullable final String basePath) throws Exception {
        final String pathPrefix = basePathToAssertablePrefix(basePath);

        for (Tuple<String, String> resource : resources) {
            final MockRequest getRequest = webServer.takeRequest();

            assertThat(getRequest.getMethod(), equalTo("GET"));
            assertThat(getRequest.getUri().getPath(), equalTo(pathPrefix + resourcePrefix + resource.v1()));
            assertMonitorVersionQueryString(getRequest.getUri().getQuery(), Collections.emptyMap());
            assertHeaders(getRequest, customHeaders);

            if (alreadyExists == false) {
                final MockRequest putRequest = webServer.takeRequest();

                assertThat(putRequest.getMethod(), equalTo("PUT"));
                assertThat(putRequest.getUri().getPath(), equalTo(pathPrefix + resourcePrefix + resource.v1()));
                Map<String, String> parameters = Collections.emptyMap();
                assertMonitorVersionQueryString(putRequest.getUri().getQuery(), parameters);
                if (resourcePrefix.startsWith("/_template")) {
                    assertThat(putRequest.getBody(), equalTo(getExternalTemplateRepresentation(resource.v2())));
                } else {
                    assertThat(putRequest.getBody(), equalTo(resource.v2()));
                }
                assertHeaders(putRequest, customHeaders);
            }
        }
    }

    private void assertMonitorVersionQueryString(String query, final Map<String, String> parameters) {
        Map<String, String> expectedQueryStringMap = new HashMap<>();
        RestUtils.decodeQueryString(query, 0, expectedQueryStringMap);

        Map<String, String> resourceVersionQueryStringMap = new HashMap<>();
        RestUtils.decodeQueryString(resourceVersionQueryString(), 0, resourceVersionQueryStringMap);

        Map<String, String> actualQueryStringMap = new HashMap<>();
        actualQueryStringMap.putAll(resourceVersionQueryStringMap);
        actualQueryStringMap.putAll(parameters);

        assertEquals(expectedQueryStringMap, actualQueryStringMap);
    }

    private void assertMonitorWatches(final MockWebServer webServer,
                                      final boolean remoteClusterAllowsWatcher, final boolean currentLicenseAllowsWatcher,
                                      final boolean alreadyExists,
                                      @Nullable final Map<String, String[]> customHeaders,
                                      @Nullable final String basePath) {
        final String pathPrefix = basePathToAssertablePrefix(basePath);
        MockRequest request;

        request = webServer.takeRequest();

        // GET /_xpack
        assertThat(request.getMethod(), equalTo("GET"));
        assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/_xpack"));
        assertThat(request.getUri().getQuery(), equalTo(watcherCheckQueryString()));
        assertHeaders(request, customHeaders);

        if (remoteClusterAllowsWatcher) {
            for (final Tuple<String, String> watch : monitoringWatches()) {
                final String uniqueWatchId = ClusterAlertsUtil.createUniqueWatchId(clusterService(), watch.v1());

                request = webServer.takeRequest();

                // GET / PUT if we are allowed to use it
                if (currentLicenseAllowsWatcher && clusterAlertBlacklist.contains(watch.v1()) == false) {
                    assertThat(request.getMethod(), equalTo("GET"));
                    assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/_watcher/watch/" + uniqueWatchId));
                    assertThat(request.getUri().getQuery(), equalTo(resourceClusterAlertQueryString()));
                    assertHeaders(request, customHeaders);

                    if (alreadyExists == false) {
                        request = webServer.takeRequest();

                        assertThat(request.getMethod(), equalTo("PUT"));
                        assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/_watcher/watch/" + uniqueWatchId));
                        assertThat(request.getUri().getQuery(), equalTo(resourceClusterAlertQueryString()));
                        assertThat(request.getBody(), equalTo(watch.v2()));
                        assertHeaders(request, customHeaders);
                    }
                    // DELETE if we're not allowed to use it
                } else {
                    assertThat(request.getMethod(), equalTo("DELETE"));
                    assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/_watcher/watch/" + uniqueWatchId));
                    assertThat(request.getUri().getQuery(), equalTo(resourceClusterAlertQueryString()));
                    assertHeaders(request, customHeaders);
                }
            }
        }
    }

    private MockRequest assertBulk(final MockWebServer webServer) throws Exception {
        return assertBulk(webServer, -1);
    }

    private MockRequest assertBulk(final MockWebServer webServer, final int docs) throws Exception {
        return assertBulk(webServer, docs, null, null);
    }

    private MockRequest assertBulk(final MockWebServer webServer, final int docs,
                                   @Nullable final Map<String, String[]> customHeaders, @Nullable final String basePath)
        throws Exception {
        final String pathPrefix = basePathToAssertablePrefix(basePath);
        final MockRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("POST"));
        assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/_bulk"));
        assertThat(request.getUri().getQuery(), equalTo(bulkQueryString()));
        assertHeaders(request, customHeaders);

        if (docs != -1) {
            assertBulkRequest(request.getBody(), docs);
        }

        return request;
    }

    private void assertHeaders(final MockRequest request, final Map<String, String[]> customHeaders) {
        if (customHeaders != null) {
            for (final Map.Entry<String, String[]> entry : customHeaders.entrySet()) {
                final String header = entry.getKey();
                final String[] values = entry.getValue();

                final List<String> headerValues = request.getHeaders().get(header);

                if (values.length > 0) {
                    assertThat(headerValues, hasSize(values.length));
                    assertThat(headerValues, containsInAnyOrder(values));
                }
            }
        }
    }
}
