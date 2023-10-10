/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import com.unboundid.util.Base64;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.LocalStateMonitoring;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.MonitoringTemplateRegistry;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.LAST_UPDATED_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.indexName;
import static org.elasticsearch.xpack.monitoring.exporter.http.ClusterAlertHttpResource.CLUSTER_ALERT_VERSION_PARAMETERS;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.FILTER_PATH_RESOURCE_VERSION;
import static org.elasticsearch.xpack.monitoring.exporter.http.WatcherExistsHttpResource.WATCHER_CHECK_PARAMETERS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@SuppressWarnings("HiddenField")
public class HttpExporterIT extends MonitoringIntegTestCase {

    private final List<String> clusterAlertBlacklist = rarely()
        ? randomSubsetOf(Arrays.asList(ClusterAlertsUtil.WATCH_IDS))
        : Collections.emptyList();
    private final boolean remoteClusterAllowsWatcher = randomBoolean();
    private final boolean currentLicenseAllowsWatcher = true;
    private final boolean watcherAlreadyExists = randomBoolean();
    private final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
    private final String userName = "elasticuser";
    private final MonitoringMigrationCoordinator coordinator = new MonitoringMigrationCoordinator();

    private MockWebServer webServer;

    private MockSecureSettings mockSecureSettings = new MockSecureSettings();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {

        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MonitoringService.INTERVAL.getKey(), MonitoringService.MIN_INTERVAL)
            // we do this by default in core, but for monitoring this isn't needed and only adds noise.
            .put("indices.lifecycle.history_index_enabled", false)
            .put("index.store.mock.check_index_on_close", false);
        return builder.build();
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
    protected boolean ignoreExternalCluster() {
        return true;
    }

    private Settings.Builder secureSettings(String password) {
        mockSecureSettings.setString("xpack.monitoring.exporters._http.auth.secure_password", password);
        return baseSettings().setSecureSettings(mockSecureSettings);
    }

    private Settings.Builder baseSettings() {
        return Settings.builder()
            .put("xpack.monitoring.exporters._http.enabled", false)
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.ssl.truststore.password", "foobar") // ensure that ssl can be used by settings
            .put("xpack.monitoring.exporters._http.headers.ignored", "value") // ensure that headers can be used by settings
            .put("xpack.monitoring.exporters._http.host", getFormattedAddress(webServer))
            .put("xpack.monitoring.exporters._http.cluster_alerts.management.enabled", true)
            .putList("xpack.monitoring.exporters._http.cluster_alerts.management.blacklist", clusterAlertBlacklist)
            .put("xpack.monitoring.exporters._http.auth.username", userName);
    }

    public void testExport() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        final int nbDocs = randomIntBetween(1, 25);
        export(settings, newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        assertBulk(webServer, nbDocs);
    }

    public void testSecureSetting() throws Exception {
        final String securePassword1 = "elasticpass";
        final String securePassword2 = "anotherpassword";
        final String authHeaderValue = Base64.encode(userName + ":" + securePassword1);
        final String authHeaderValue2 = Base64.encode(userName + ":" + securePassword2);

        Settings settings = secureSettings(securePassword1).build();
        PluginsService pluginsService = internalCluster().getInstances(PluginsService.class).iterator().next();
        LocalStateMonitoring localStateMonitoring = pluginsService.filterPlugins(LocalStateMonitoring.class).iterator().next();
        localStateMonitoring.getMonitoring().reload(settings);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        final int nbDocs = randomIntBetween(1, 25);
        export(settings, newRandomMonitoringDocs(nbDocs));
        assertEquals(webServer.takeRequest().getHeader("Authorization").replace("Basic", "").replace(" ", ""), authHeaderValue);
        webServer.clearRequests();

        settings = secureSettings(securePassword2).build();
        localStateMonitoring.getMonitoring().reload(settings);
        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        export(settings, newRandomMonitoringDocs(nbDocs));
        assertEquals(webServer.takeRequest().getHeader("Authorization").replace("Basic", "").replace(" ", ""), authHeaderValue2);
    }

    public void testExportWithHeaders() throws Exception {
        final String headerValue = randomAlphaOfLengthBetween(3, 9);
        final String[] array = generateRandomStringArray(2, 4, false, false);

        final Map<String, String[]> headers = new HashMap<>();

        headers.put("X-Cloud-Cluster", new String[] { headerValue });
        headers.put("X-Found-Cluster", new String[] { headerValue });
        headers.put("Array-Check", array);

        final Settings settings = baseSettings().put("xpack.monitoring.exporters._http.headers.X-Cloud-Cluster", headerValue)
            .put("xpack.monitoring.exporters._http.headers.X-Found-Cluster", headerValue)
            .putList("xpack.monitoring.exporters._http.headers.Array-Check", array)
            .build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        final int nbDocs = randomIntBetween(1, 25);
        export(settings, newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(
            webServer,
            true,
            remoteClusterAllowsWatcher,
            currentLicenseAllowsWatcher,
            watcherAlreadyExists,
            headers,
            null
        );
        assertBulk(webServer, nbDocs, headers, null);
    }

    public void testExportWithBasePath() throws Exception {
        final boolean useHeaders = randomBoolean();

        final String headerValue = randomAlphaOfLengthBetween(3, 9);
        final String[] array = generateRandomStringArray(2, 4, false, false);

        final Map<String, String[]> headers = new HashMap<>();

        if (useHeaders) {
            headers.put("X-Cloud-Cluster", new String[] { headerValue });
            headers.put("X-Found-Cluster", new String[] { headerValue });
            headers.put("Array-Check", array);
        }

        String basePath = "path/to";

        if (randomBoolean()) {
            basePath += "/something";

            if (rarely()) {
                basePath += "/proxied";
            }
        }

        if (randomBoolean()) {
            basePath = "/" + basePath;
        }

        final Settings.Builder builder = baseSettings().put(
            "xpack.monitoring.exporters._http.proxy.base_path",
            basePath + (randomBoolean() ? "/" : "")
        );

        if (useHeaders) {
            builder.put("xpack.monitoring.exporters._http.headers.X-Cloud-Cluster", headerValue)
                .put("xpack.monitoring.exporters._http.headers.X-Found-Cluster", headerValue)
                .putList("xpack.monitoring.exporters._http.headers.Array-Check", array);
        }

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false}");

        final int nbDocs = randomIntBetween(1, 25);
        export(builder.build(), newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(
            webServer,
            true,
            remoteClusterAllowsWatcher,
            currentLicenseAllowsWatcher,
            watcherAlreadyExists,
            headers,
            basePath
        );
        assertBulk(webServer, nbDocs, headers, basePath);
    }

    public void testHostChangeReChecksTemplate() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false}");

        export(settings, Collections.singletonList(newRandomMonitoringDoc()));

        assertMonitorResources(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        assertBulk(webServer);

        try (MockWebServer secondWebServer = createMockWebServer()) {

            final Settings newSettings = Settings.builder()
                .put(settings)
                .putList("xpack.monitoring.exporters._http.host", getFormattedAddress(secondWebServer))
                .build();

            enqueueGetClusterVersionResponse(secondWebServer, Version.CURRENT);
            enqueueSetupResponses(secondWebServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
            enqueueResponse(secondWebServer, 200, "{\"errors\": false}");

            // second event
            export(newSettings, Collections.singletonList(newRandomMonitoringDoc()));

            assertMonitorResources(secondWebServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
            assertBulk(secondWebServer);
        }
    }

    public void testUnsupportedClusterVersion() throws Exception {
        final Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", getFormattedAddress(webServer))
            .build();

        // returning an unsupported cluster version
        enqueueGetClusterVersionResponse(
            randomFrom(
                Version.fromString("0.18.0"),
                Version.fromString("1.0.0"),
                Version.fromString("1.4.0"),
                Version.fromString("2.4.0"),
                Version.fromString("5.0.0"),
                Version.fromString("5.4.0")
            )
        );

        // ensure that the exporter is not able to be used
        try (HttpExporter exporter = createHttpExporter(settings)) {
            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);

            final ActionListener<ExportBulk> listener = ActionTestUtils.assertNoFailureListener(bulk -> {
                assertNull(bulk);
                awaitResponseAndClose.countDown();
            });

            exporter.openBulk(listener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }

        assertThat(webServer.requests(), hasSize(1));

        assertMonitorVersion(webServer);
    }

    public void testRemoteTemplatesNotPresent() throws Exception {
        final Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", getFormattedAddress(webServer))
            .build();

        // returning an unsupported cluster version
        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, false, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);

        // ensure that the exporter is not able to be used
        try (HttpExporter exporter = createHttpExporter(settings)) {
            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);

            final ActionListener<ExportBulk> listener = ActionTestUtils.assertNoFailureListener(bulk -> {
                assertNull(bulk);
                awaitResponseAndClose.countDown();
            });

            exporter.openBulk(listener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }

        // 1) version request, 2) first template get request
        assertThat(webServer.requests(), hasSize(2));

        assertMonitorVersion(webServer);
        assertMonitorTemplates(webServer, false, null, null);
    }

    public void testDynamicIndexFormatChange() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        MonitoringDoc doc = newRandomMonitoringDoc();
        export(settings, Collections.singletonList(doc));

        assertMonitorResources(webServer, true, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        MockRequest recordedRequest = assertBulk(webServer);

        DateFormatter formatter = DateFormatter.forPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
        String indexName = indexName(formatter, doc.getSystem(), doc.getTimestamp());

        byte[] bytes = recordedRequest.getBody().getBytes(StandardCharsets.UTF_8);
        Map<String, Object> data = XContentHelper.convertToMap(new BytesArray(bytes), false, XContentType.JSON).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) data.get("index");
        assertThat(index.get("_index"), equalTo(indexName));

        String newTimeFormat = randomFrom("yy", "yyyy", "yyyy.MM", "yyyy-MM", "MM.yyyy", "MM");

        final Settings newSettings = Settings.builder()
            .put(settings)
            .put("xpack.monitoring.exporters._http.index.name.time_format", newTimeFormat)
            .build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer, true, true, true, true);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        doc = newRandomMonitoringDoc();
        export(newSettings, Collections.singletonList(doc));

        DateFormatter newTimeFormatter = DateFormatter.forPattern(newTimeFormat).withZone(ZoneOffset.UTC);

        String expectedMonitoringIndex = ".monitoring-es-"
            + TEMPLATE_VERSION
            + "-"
            + newTimeFormatter.format(Instant.ofEpochMilli(doc.getTimestamp()));

        assertMonitorResources(webServer, true, true, true, true);
        recordedRequest = assertBulk(webServer);

        bytes = recordedRequest.getBody().getBytes(StandardCharsets.UTF_8);
        data = XContentHelper.convertToMap(new BytesArray(bytes), false, XContentType.JSON).v2();
        @SuppressWarnings("unchecked")
        final Map<String, Object> newIndex = (Map<String, Object>) data.get("index");
        assertThat(newIndex.get("_index"), equalTo(expectedMonitoringIndex));
    }

    private void assertMonitorVersion(final MockWebServer webServer) throws Exception {
        assertMonitorVersion(webServer, null, null);
    }

    private void assertMonitorVersion(
        final MockWebServer webServer,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) throws Exception {
        final MockRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("GET"));
        final String pathPrefix = basePathToAssertablePrefix(basePath);
        if (Strings.isEmpty(pathPrefix) == false) {
            assertThat(request.getUri().getPath(), equalTo(pathPrefix + "/"));
        }
        assertThat(request.getUri().getQuery(), equalTo("filter_path=version.number"));
        assertHeaders(request, customHeaders);
    }

    private void assertMonitorResources(
        final MockWebServer webServer,
        final boolean templateAlreadyExists,
        final boolean remoteClusterAllowsWatcher,
        final boolean currentLicenseAllowsWatcher,
        final boolean watcherAlreadyExists
    ) throws Exception {
        assertMonitorResources(
            webServer,
            templateAlreadyExists,
            remoteClusterAllowsWatcher,
            currentLicenseAllowsWatcher,
            watcherAlreadyExists,
            null,
            null
        );
    }

    private void assertMonitorResources(
        final MockWebServer webServer,
        final boolean templateAlreadyExists,
        final boolean remoteClusterAllowsWatcher,
        final boolean currentLicenseAllowsWatcher,
        final boolean watcherAlreadyExists,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) throws Exception {
        assertMonitorVersion(webServer, customHeaders, basePath);
        assertMonitorTemplates(webServer, templateAlreadyExists, customHeaders, basePath);
        assertMonitorWatches(
            webServer,
            remoteClusterAllowsWatcher,
            currentLicenseAllowsWatcher,
            watcherAlreadyExists,
            customHeaders,
            basePath
        );
    }

    private void assertMonitorTemplates(
        final MockWebServer webServer,
        final boolean alreadyExists,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) throws Exception {
        final String resourcePrefix = "/_template/";
        final String pathPrefix = basePathToAssertablePrefix(basePath);

        for (String resource : MonitoringTemplateRegistry.TEMPLATE_NAMES) {
            final MockRequest getRequest = webServer.takeRequest();

            assertThat(getRequest.getMethod(), equalTo("GET"));
            assertThat(getRequest.getUri().getPath(), equalTo(pathPrefix + resourcePrefix + resource));
            assertMonitorVersionQueryString(getRequest.getUri().getQuery(), Collections.emptyMap());
            assertHeaders(getRequest, customHeaders);

            if (alreadyExists == false) {
                // If the templates do not exist already, the resources simply short circuit on the first missing template
                // and notifies the exporter to wait.
                break;
            }
        }
    }

    private void assertMonitorVersionResource(
        final MockWebServer webServer,
        final boolean alreadyExists,
        final String resourcePrefix,
        final List<Tuple<String, String>> resources,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) throws Exception {
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

    private void assertMonitorWatches(
        final MockWebServer webServer,
        final boolean remoteClusterAllowsWatcher,
        final boolean currentLicenseAllowsWatcher,
        final boolean alreadyExists,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) {
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

    private MockRequest assertBulk(
        final MockWebServer webServer,
        final int docs,
        @Nullable final Map<String, String[]> customHeaders,
        @Nullable final String basePath
    ) throws Exception {
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

    private HttpExporter createHttpExporter(final Settings settings) {
        final Exporter.Config config = new Exporter.Config("_http", "http", settings, clusterService(), TestUtils.newTestLicenseState());

        final Environment env = TestEnvironment.newEnvironment(buildEnvSettings(settings));
        return new HttpExporter(config, new SSLService(env), new ThreadContext(settings), coordinator);
    }

    private void export(final Settings settings, final Collection<MonitoringDoc> docs) throws Exception {
        // wait until the cluster is ready (this is done at the "Exporters" level)
        assertBusy(() -> assertThat(clusterService().state().version(), not(ClusterState.UNKNOWN_VERSION)));

        try (HttpExporter exporter = createHttpExporter(settings)) {
            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);

            exporter.openBulk(ActionTestUtils.assertNoFailureListener(exportBulk -> {
                final HttpExportBulk bulk = (HttpExportBulk) exportBulk;

                assertThat("Bulk should never be null after the exporter is ready", bulk, notNullValue());

                final ActionListener<Void> listener = ActionTestUtils.assertNoFailureListener(ignored -> awaitResponseAndClose.countDown());

                bulk.add(docs);
                bulk.flush(listener);
            }));

            // block until the bulk responds
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    private MonitoringDoc newRandomMonitoringDoc() {
        String clusterUUID = internalCluster().getClusterName();
        long timestamp = System.currentTimeMillis();
        long intervalMillis = randomNonNegativeLong();
        MonitoringDoc.Node sourceNode = MonitoringTestUtils.randomMonitoringNode(random());

        return new IndexRecoveryMonitoringDoc(
            clusterUUID,
            timestamp,
            intervalMillis,
            sourceNode,
            new RecoveryResponse(0, 0, 0, null, null)
        );
    }

    private List<MonitoringDoc> newRandomMonitoringDocs(int nb) {
        List<MonitoringDoc> docs = new ArrayList<>(nb);
        for (int i = 0; i < nb; i++) {
            docs.add(newRandomMonitoringDoc());
        }
        return docs;
    }

    private String basePathToAssertablePrefix(@Nullable String basePath) {
        if (basePath == null) {
            return "";
        }
        basePath = basePath.startsWith("/") ? basePath : "/" + basePath;
        return basePath;
    }

    private String resourceClusterAlertQueryString() {
        return "filter_path=" + CLUSTER_ALERT_VERSION_PARAMETERS.get("filter_path");
    }

    private String resourceVersionQueryString() {
        return "filter_path=" + FILTER_PATH_RESOURCE_VERSION;
    }

    private String watcherCheckQueryString() {
        return "filter_path=" + WATCHER_CHECK_PARAMETERS.get("filter_path");
    }

    private String bulkQueryString() {

        return "filter_path=" + "errors,items.*.error";
    }

    private void enqueueGetClusterVersionResponse(Version v) throws IOException {
        enqueueGetClusterVersionResponse(webServer, v);
    }

    private void enqueueGetClusterVersionResponse(MockWebServer mockWebServer, Version v) throws IOException {
        mockWebServer.enqueue(
            new MockResponse().setResponseCode(200)
                .setBody(
                    BytesReference.bytes(
                        jsonBuilder().startObject().startObject("version").field("number", v.toString()).endObject().endObject()
                    ).utf8ToString()
                )
        );
    }

    private void enqueueSetupResponses(
        final MockWebServer webServer,
        final boolean templatesAlreadyExists,
        final boolean remoteClusterAllowsWatcher,
        final boolean currentLicenseAllowsWatcher,
        final boolean watcherAlreadyExists
    ) throws IOException {
        enqueueTemplateResponses(webServer, templatesAlreadyExists);
        enqueueWatcherResponses(webServer, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
    }

    private void enqueueTemplateResponses(final MockWebServer webServer, final boolean alreadyExists) throws IOException {
        if (alreadyExists) {
            enqueueTemplateResponsesExistsAlready(webServer);
        } else {
            enqueueTemplateResponsesDoesNotExistYet(webServer);
        }
    }

    private void enqueueTemplateResponsesDoesNotExistYet(final MockWebServer webServer) throws IOException {
        enqueueVersionedResourceResponsesDoesNotExistYet(Arrays.asList(MonitoringTemplateRegistry.TEMPLATE_NAMES), webServer);
    }

    private void enqueueTemplateResponsesExistsAlready(final MockWebServer webServer) throws IOException {
        enqueueVersionedResourceResponsesExistsAlready(Arrays.asList(MonitoringTemplateRegistry.TEMPLATE_NAMES), webServer);
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

    private void enqueueWatcherResponses(
        final MockWebServer webServer,
        final boolean remoteClusterAllowsWatcher,
        final boolean currentLicenseAllowsWatcher,
        final boolean alreadyExists
    ) throws IOException {
        // if the remote cluster doesn't allow watcher, then we only check for it and we're done
        if (remoteClusterAllowsWatcher) {
            // X-Pack exists and Watcher can be used
            enqueueResponse(webServer, 200, """
                {"features":{"watcher":{"available":true,"enabled":true}}}""");

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
                final String responseBody = randomFrom("""
                    {"features":{"watcher":{"available":false,"enabled":true}}}""", """
                    {"features":{"watcher":{"available":true,"enabled":false}}}""", "{}");

                enqueueResponse(webServer, 200, responseBody);
            } else {
                // X-Pack is not installed
                enqueueResponse(webServer, 404, "{}");
            }
        }
    }

    private void enqueueClusterAlertResponsesDoesNotExistYet(final MockWebServer webServer) throws IOException {
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

    private void assertBulkRequest(String requestBody, int numberOfActions) throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(
            new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)),
            null,
            XContentType.JSON
        );
        assertThat(bulkRequest.numberOfActions(), equalTo(numberOfActions));
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests()) {
            assertThat(actionRequest, instanceOf(IndexRequest.class));
        }
    }

    private String getFormattedAddress(MockWebServer server) {
        return server.getHostName() + ":" + server.getPort();
    }

    private MockWebServer createMockWebServer() throws IOException {
        MockWebServer server = new MockWebServer();
        server.start();
        return server;
    }

    private String getExternalTemplateRepresentation(String internalRepresentation) throws IOException {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, internalRepresentation)
        ) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            IndexTemplateMetadata.Builder.removeType(IndexTemplateMetadata.Builder.fromXContent(parser, ""), builder);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
