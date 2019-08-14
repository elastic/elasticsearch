/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
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
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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

@ESIntegTestCase.ClusterScope(scope = Scope.TEST,
                              numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HttpExporterIT extends MonitoringIntegTestCase {

    private final List<String> clusterAlertBlacklist =
            rarely() ? randomSubsetOf(Arrays.asList(ClusterAlertsUtil.WATCH_IDS)) : Collections.emptyList();
    private final boolean templatesExistsAlready = randomBoolean();
    private final boolean includeOldTemplates = randomBoolean();
    private final boolean pipelineExistsAlready = randomBoolean();
    private final boolean remoteClusterAllowsWatcher = randomBoolean();
    private final boolean currentLicenseAllowsWatcher = true;
    private final boolean watcherAlreadyExists = randomBoolean();
    private final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());

    private MockWebServer webServer;

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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // we create and disable the exporter to avoid the cluster actually using it (thus speeding up tests)
        // we make an exporter on demand per test
        return Settings.builder()
                       .put(super.nodeSettings(nodeOrdinal))
                       .put("xpack.monitoring.exporters._http.type", "http")
                       .put("xpack.monitoring.exporters._http.ssl.truststore.password", "foobar") // ensure that ssl can be used by settings
                       .put("xpack.monitoring.exporters._http.headers.ignored", "value") // ensure that headers can be used by settings
                       .put("xpack.monitoring.exporters._http.enabled", false)
                       .build();
    }

    private Settings.Builder baseSettings() {
        return Settings.builder()
                       .put("xpack.monitoring.exporters._http.type", "http")
                       .put("xpack.monitoring.exporters._http.host", getFormattedAddress(webServer))
                       .putList("xpack.monitoring.exporters._http.cluster_alerts.management.blacklist", clusterAlertBlacklist)
                       .put("xpack.monitoring.exporters._http.index.template.create_legacy_templates", includeOldTemplates);
    }

    public void testExport() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer,
                              templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                              remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        final int nbDocs = randomIntBetween(1, 25);
        export(settings, newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(webServer,
                               templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                               remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        assertBulk(webServer, nbDocs);
    }

    public void testExportWithHeaders() throws Exception {
        final String headerValue = randomAlphaOfLengthBetween(3, 9);
        final String[] array = generateRandomStringArray(2, 4, false, false);

        final Map<String, String[]> headers = new HashMap<>();

        headers.put("X-Cloud-Cluster", new String[] { headerValue });
        headers.put("X-Found-Cluster", new String[] { headerValue });
        headers.put("Array-Check", array);

        final Settings settings = baseSettings()
                .put("xpack.monitoring.exporters._http.headers.X-Cloud-Cluster", headerValue)
                .put("xpack.monitoring.exporters._http.headers.X-Found-Cluster", headerValue)
                .putList("xpack.monitoring.exporters._http.headers.Array-Check", array)
                .build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer,
                              templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                              remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        final int nbDocs = randomIntBetween(1, 25);
        export(settings, newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(webServer,
                               templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                               remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists,
                               headers, null);
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

        final Settings.Builder builder = baseSettings()
                .put("xpack.monitoring.exporters._http.proxy.base_path", basePath + (randomBoolean() ? "/" : ""));

        if (useHeaders) {
            builder.put("xpack.monitoring.exporters._http.headers.X-Cloud-Cluster", headerValue)
                    .put("xpack.monitoring.exporters._http.headers.X-Found-Cluster", headerValue)
                    .putList("xpack.monitoring.exporters._http.headers.Array-Check", array);
        }

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer,
                              templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                              remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false}");

        final int nbDocs = randomIntBetween(1, 25);
        export(builder.build(), newRandomMonitoringDocs(nbDocs));

        assertMonitorResources(webServer,
                               templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                               remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists,
                               headers, basePath);
        assertBulk(webServer, nbDocs, headers, basePath);
    }

    public void testHostChangeReChecksTemplate() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer,
                              templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                              remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false}");

        export(settings, Collections.singletonList(newRandomMonitoringDoc()));

        assertMonitorResources(webServer,
                               templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                               remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        assertBulk(webServer);

        try (MockWebServer secondWebServer = createMockWebServer()) {
            String missingTemplate = null;

            final Settings newSettings = Settings.builder()
                    .put(settings)
                    .putList("xpack.monitoring.exporters._http.host", getFormattedAddress(secondWebServer))
                    .build();

            enqueueGetClusterVersionResponse(secondWebServer, Version.CURRENT);
            // pretend that one of the templates is missing
            for (Tuple<String, String> template : monitoringTemplates(includeOldTemplates)) {
                if (missingTemplate != null) {
                    enqueueResponse(secondWebServer, 200, "{\"" + template.v1() + "\":{\"version\":" + LAST_UPDATED_VERSION + "}}");
                } else {
                    missingTemplate = template.v1();

                    enqueueResponse(secondWebServer, 404, "template [" + template.v1() + "] does not exist");
                    enqueueResponse(secondWebServer, 201, "template [" + template.v1() + "] created");
                }
            }
            // opposite of if it existed before
            enqueuePipelineResponses(secondWebServer, !pipelineExistsAlready);
            enqueueWatcherResponses(secondWebServer, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
            enqueueResponse(secondWebServer, 200, "{\"errors\": false}");

            // second event
            export(newSettings, Collections.singletonList(newRandomMonitoringDoc()));

            assertMonitorVersion(secondWebServer);

            String resourcePrefix = "/_template/";
            for (Tuple<String, String> template : monitoringTemplates(includeOldTemplates)) {
                MockRequest recordedRequest = secondWebServer.takeRequest();
                assertThat(recordedRequest.getMethod(), equalTo("GET"));
                assertThat(recordedRequest.getUri().getPath(), equalTo(resourcePrefix + template.v1()));
                assertMonitorVersionQueryString(recordedRequest.getUri().getQuery(), Collections.emptyMap());

                if (missingTemplate.equals(template.v1())) {
                    recordedRequest = secondWebServer.takeRequest();
                    assertThat(recordedRequest.getMethod(), equalTo("PUT"));
                    assertThat(recordedRequest.getUri().getPath(), equalTo(resourcePrefix + template.v1()));
                    assertMonitorVersionQueryString(recordedRequest.getUri().getQuery(), Collections.emptyMap());
                    assertThat(recordedRequest.getBody(), equalTo(getExternalTemplateRepresentation(template.v2())));
                }
            }
            assertMonitorPipelines(secondWebServer, !pipelineExistsAlready, null, null);
            assertMonitorWatches(secondWebServer, remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists,
                                 null, null);
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
                randomFrom(Version.fromString("0.18.0"),
                           Version.fromString("1.0.0"),
                           Version.fromString("1.4.0"),
                           Version.fromString("2.4.0"),
                           Version.fromString("5.0.0"),
                           Version.fromString("5.4.0")));

        // ensure that the exporter is not able to be used
        try (HttpExporter exporter = createHttpExporter(settings)) {
            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);

            final ActionListener<ExportBulk> listener = ActionListener.wrap(
                bulk -> {
                    assertNull(bulk);

                    awaitResponseAndClose.countDown();
                },
                e -> fail(e.getMessage())
            );

            exporter.openBulk(listener);

            // wait for it to actually respond
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }

        assertThat(webServer.requests(), hasSize(1));

        assertMonitorVersion(webServer);
    }

    public void testDynamicIndexFormatChange() throws Exception {
        final Settings settings = baseSettings().build();

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueSetupResponses(webServer,
                              templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                              remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        MonitoringDoc doc = newRandomMonitoringDoc();
        export(settings, Collections.singletonList(doc));

        assertMonitorResources(webServer,
                               templatesExistsAlready, includeOldTemplates, pipelineExistsAlready,
                               remoteClusterAllowsWatcher, currentLicenseAllowsWatcher, watcherAlreadyExists);
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
        enqueueSetupResponses(webServer, true, includeOldTemplates, true,
                              true, true, true);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        doc = newRandomMonitoringDoc();
        export(newSettings, Collections.singletonList(doc));

        DateFormatter newTimeFormatter = DateFormatter.forPattern(newTimeFormat).withZone(ZoneOffset.UTC);

        String expectedMonitoringIndex = ".monitoring-es-" + TEMPLATE_VERSION + "-"
                + newTimeFormatter.format(Instant.ofEpochMilli(doc.getTimestamp()));

        assertMonitorResources(webServer, true, includeOldTemplates, true,
                               true, true, true);
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

    private HttpExporter createHttpExporter(final Settings settings) {
        final Exporter.Config config =
                new Exporter.Config("_http", "http", settings, clusterService(), new XPackLicenseState(Settings.EMPTY));

        return new HttpExporter(config, new SSLService(settings, environment), new ThreadContext(settings));
    }

    private void export(final Settings settings, final Collection<MonitoringDoc> docs) throws Exception {
        // wait until the cluster is ready (this is done at the "Exporters" level)
        assertBusy(() -> assertThat(clusterService().state().version(), not(ClusterState.UNKNOWN_VERSION)));

        try (HttpExporter exporter = createHttpExporter(settings)) {
            final CountDownLatch awaitResponseAndClose = new CountDownLatch(1);

            exporter.openBulk(ActionListener.wrap(exportBulk -> {
                final HttpExportBulk bulk = (HttpExportBulk)exportBulk;

                assertThat("Bulk should never be null after the exporter is ready", bulk, notNullValue());

                final ActionListener<Void> listener = ActionListener.wrap(
                    ignored -> awaitResponseAndClose.countDown(),
                    e -> fail(e.getMessage())
                );

                bulk.add(docs);
                bulk.flush(listener);
            }, e -> fail("Failed to create HttpExportBulk")));

            // block until the bulk responds
            assertTrue(awaitResponseAndClose.await(15, TimeUnit.SECONDS));
        }
    }

    private MonitoringDoc newRandomMonitoringDoc() {
        String clusterUUID = internalCluster().getClusterName();
        long timestamp = System.currentTimeMillis();
        long intervalMillis = randomNonNegativeLong();
        MonitoringDoc.Node sourceNode = MonitoringTestUtils.randomMonitoringNode(random());

        return new IndexRecoveryMonitoringDoc(clusterUUID, timestamp, intervalMillis, sourceNode,
            new RecoveryResponse(0, 0, 0, null, null));
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
        basePath = basePath.startsWith("/")? basePath : "/" + basePath;
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
        final String pipelineName = MonitoringTemplateUtils.pipelineName(TEMPLATE_VERSION);

        return "pipeline=" + pipelineName + "&filter_path=" + "errors,items.*.error";
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
            // otherwise we need to delete them from the remote cluster
            } else {
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
            // X-Pack is not installed
            } else {
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

    private void assertBulkRequest(String requestBody, int numberOfActions) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest()
                .add(new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)), null, null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(numberOfActions));
        for (DocWriteRequest actionRequest : bulkRequest.requests()) {
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

    private List<String> monitoringTemplateNames(final boolean includeOldTemplates) {
        return includeOldTemplates ? monitoringTemplateNamesWithOldTemplates() : monitoringTemplateNames();
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

    private String getExternalTemplateRepresentation(String internalRepresentation) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, internalRepresentation)) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            IndexTemplateMetaData.Builder.removeType(IndexTemplateMetaData.Builder.fromXContent(parser, ""), builder);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
