/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.exporter.http;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.agent.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.xpack.monitoring.agent.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.agent.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.agent.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.agent.resolver.bulk.MonitoringBulkTimestampedResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class HttpExporterTests extends MonitoringIntegTestCase {

    private int webPort;
    private MockWebServer webServer;

    @Before
    public void startWebservice() throws Exception {
        for (webPort = 9250; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                QueueDispatcher dispatcher = new QueueDispatcher();
                dispatcher.setFailFast(true);
                webServer.setDispatcher(dispatcher);
                webServer.start(webPort);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    @After
    public void cleanup() throws Exception {
        webServer.shutdown();
    }

    private int expectedTemplateAndPipelineCalls(final boolean templateAlreadyExists, final boolean pipelineAlreadyExists) {
        return expectedTemplateCalls(templateAlreadyExists) + expectedPipelineCalls(pipelineAlreadyExists);
    }

    private int expectedTemplateCalls(final boolean alreadyExists) {
        return monitoringTemplates().size() * (alreadyExists ? 1 : 2);
    }

    private int expectedPipelineCalls(final boolean alreadyExists) {
        return alreadyExists ? 1 : 2;
    }

    private void assertMonitorVersion(final MockWebServer webServer) throws Exception {
        assertMonitorVersion(webServer, null);
    }

    private void assertMonitorVersion(final MockWebServer webServer, @Nullable final Map<String, String[]> customHeaders)
            throws Exception {
        RecordedRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("GET"));
        assertThat(request.getPath(), equalTo("/"));
        assertHeaders(request, customHeaders);
    }

    private void assertMonitorTemplatesAndPipeline(final MockWebServer webServer,
                                                   final boolean templateAlreadyExists, final boolean pipelineAlreadyExists)
            throws Exception {
        assertMonitorTemplatesAndPipeline(webServer, templateAlreadyExists, pipelineAlreadyExists, null);
    }

    private void assertMonitorTemplatesAndPipeline(final MockWebServer webServer,
                                                   final boolean templateAlreadyExists, final boolean pipelineAlreadyExists,
                                                   @Nullable final Map<String, String[]> customHeaders) throws Exception {
        assertMonitorVersion(webServer, customHeaders);
        assertMonitorTemplates(webServer, templateAlreadyExists, customHeaders);
        assertMonitorPipelines(webServer, pipelineAlreadyExists, customHeaders);
    }

    private void assertMonitorTemplates(final MockWebServer webServer, final boolean alreadyExists,
                                        @Nullable final Map<String, String[]> customHeaders) throws Exception {
        RecordedRequest request;

        for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
            request = webServer.takeRequest();

            assertThat(request.getMethod(), equalTo("GET"));
            assertThat(request.getPath(), equalTo("/_template/" + template.getKey()));
            assertHeaders(request, customHeaders);

            if (alreadyExists == false) {
                request = webServer.takeRequest();

                assertThat(request.getMethod(), equalTo("PUT"));
                assertThat(request.getPath(), equalTo("/_template/" + template.getKey()));
                assertThat(request.getBody().readUtf8(), equalTo(template.getValue()));
                assertHeaders(request, customHeaders);
            }
        }
    }

    private void assertMonitorPipelines(final MockWebServer webServer, final boolean alreadyExists,
                                        @Nullable final Map<String, String[]> customHeaders) throws Exception {
        RecordedRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("GET"));
        assertThat(request.getPath(), equalTo("/_ingest/pipeline/" + Exporter.EXPORT_PIPELINE_NAME));
        assertHeaders(request, customHeaders);

        if (alreadyExists == false) {
            request = webServer.takeRequest();

            assertThat(request.getMethod(), equalTo("PUT"));
            assertThat(request.getPath(), equalTo("/_ingest/pipeline/" + Exporter.EXPORT_PIPELINE_NAME));
            assertThat(request.getBody().readUtf8(), equalTo(Exporter.emptyPipeline(XContentType.JSON).string()));
            assertHeaders(request, customHeaders);
        }
    }

    private RecordedRequest assertBulk(final MockWebServer webServer) throws Exception {
        return assertBulk(webServer, -1);
    }

    private RecordedRequest assertBulk(final MockWebServer webServer, final int docs) throws Exception {
        return assertBulk(webServer, docs, null);
    }


    private RecordedRequest assertBulk(final MockWebServer webServer, final int docs, @Nullable final Map<String, String[]> customHeaders)
            throws Exception {
        RecordedRequest request = webServer.takeRequest();

        assertThat(request.getMethod(), equalTo("POST"));
        assertThat(request.getPath(), equalTo("/_bulk?pipeline=" + Exporter.EXPORT_PIPELINE_NAME));
        assertHeaders(request, customHeaders);

        if (docs != -1) {
            assertBulkRequest(request.getBody(), docs);
        }

        return request;
    }

    private void assertHeaders(final RecordedRequest request, final Map<String, String[]> customHeaders) {
        if (customHeaders != null) {
            for (final Map.Entry<String, String[]> entry : customHeaders.entrySet()) {
                final String header = entry.getKey();
                final String[] values = entry.getValue();

                final List<String> headerValues = request.getHeaders().values(header);

                assertThat(header, headerValues, hasSize(values.length));
                assertThat(header, headerValues, containsInAnyOrder(values));
            }
        }
    }

    public void testExport() throws Exception {
        final boolean templatesExistsAlready = randomBoolean();
        final boolean pipelineExistsAlready = randomBoolean();
        final int expectedTemplateAndPipelineCalls = expectedTemplateAndPipelineCalls(templatesExistsAlready, pipelineExistsAlready);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueTemplateAndPipelineResponses(webServer, templatesExistsAlready, pipelineExistsAlready);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.exporters._http.update_mappings", false);

        internalCluster().startNode(builder);

        final int nbDocs = randomIntBetween(1, 25);
        export(newRandomMonitoringDocs(nbDocs));

        assertThat(webServer.getRequestCount(), equalTo(2 + expectedTemplateAndPipelineCalls));
        assertMonitorTemplatesAndPipeline(webServer, templatesExistsAlready, pipelineExistsAlready);
        assertBulk(webServer, nbDocs);
    }

    public void testExportWithHeaders() throws Exception {
        final boolean templatesExistsAlready = randomBoolean();
        final boolean pipelineExistsAlready = randomBoolean();
        final int expectedTemplateAndPipelineCalls = expectedTemplateAndPipelineCalls(templatesExistsAlready, pipelineExistsAlready);

        final String headerValue = randomAsciiOfLengthBetween(3, 9);
        final String[] array = generateRandomStringArray(2, 4, false);

        final Map<String, String[]> headers = new HashMap<>();

        headers.put("X-Cloud-Cluster", new String[] { headerValue });
        headers.put("X-Found-Cluster", new String[] { headerValue });
        headers.put("Array-Check", array);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueTemplateAndPipelineResponses(webServer, templatesExistsAlready, pipelineExistsAlready);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.exporters._http.update_mappings", false)
                .put("xpack.monitoring.exporters._http.headers.X-Cloud-Cluster", headerValue)
                .put("xpack.monitoring.exporters._http.headers.X-Found-Cluster", headerValue)
                .putArray("xpack.monitoring.exporters._http.headers.Array-Check", array);

        internalCluster().startNode(builder);

        final int nbDocs = randomIntBetween(1, 25);
        export(newRandomMonitoringDocs(nbDocs));

        assertThat(webServer.getRequestCount(), equalTo(2 + expectedTemplateAndPipelineCalls));
        assertMonitorTemplatesAndPipeline(webServer, templatesExistsAlready, pipelineExistsAlready);
        assertBulk(webServer, nbDocs, headers);
    }

    public void testDynamicHostChange() {
        // disable exporting to be able to use non valid hosts
        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", "test0");

        String nodeName = internalCluster().startNode(builder);

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.exporters._http.host", "test1")));
        assertThat(getExporter(nodeName).hosts, arrayContaining("test1"));

        // wipes the non array settings
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.exporters._http.host", "test2")
                .put("xpack.monitoring.exporters._http.host", "")));
        assertThat(getExporter(nodeName).hosts, arrayContaining("test2"));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.exporters._http.host", "test3")));
        assertThat(getExporter(nodeName).hosts, arrayContaining("test3"));
    }

    public void testHostChangeReChecksTemplate() throws Exception {
        final boolean templatesExistsAlready = randomBoolean();
        final boolean pipelineExistsAlready = randomBoolean();
        final int expectedTemplateAndPipelineCalls = expectedTemplateAndPipelineCalls(templatesExistsAlready, pipelineExistsAlready);

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.exporters._http.update_mappings", false);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueTemplateAndPipelineResponses(webServer, templatesExistsAlready, pipelineExistsAlready);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        String agentNode = internalCluster().startNode(builder);

        HttpExporter exporter = getExporter(agentNode);
        assertThat(exporter.supportedClusterVersion, is(false));
        export(Collections.singletonList(newRandomMonitoringDoc()));

        assertThat(exporter.supportedClusterVersion, is(true));
        assertThat(webServer.getRequestCount(), equalTo(2 + expectedTemplateAndPipelineCalls));
        assertMonitorTemplatesAndPipeline(webServer, templatesExistsAlready, pipelineExistsAlready);
        assertBulk(webServer);

        MockWebServer secondWebServer = null;
        int secondWebPort;

        try {
            final int expectedPipelineCalls = expectedPipelineCalls(!pipelineExistsAlready);

            for (secondWebPort = 9250; secondWebPort < 9300; secondWebPort++) {
                try {
                    secondWebServer = new MockWebServer();
                    QueueDispatcher dispatcher = new QueueDispatcher();
                    dispatcher.setFailFast(true);
                    secondWebServer.setDispatcher(dispatcher);
                    secondWebServer.start(secondWebPort);
                    break;
                } catch (BindException be) {
                    logger.warn("port [{}] was already in use trying next port", secondWebPort);
                }
            }

            assertNotNull("Unable to start the second mock web server", secondWebServer);

            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    Settings.builder().putArray("xpack.monitoring.exporters._http.host",
                            secondWebServer.getHostName() + ":" + secondWebServer.getPort())).get());

            // a new exporter is created on update, so we need to re-fetch it
            exporter = getExporter(agentNode);

            enqueueGetClusterVersionResponse(secondWebServer, Version.CURRENT);
            for (String template : monitoringTemplates().keySet()) {
                if (template.contains(MonitoringBulkTimestampedResolver.Data.DATA)) {
                    enqueueResponse(secondWebServer, 200, "template [" + template + "] exists");
                } else {
                    enqueueResponse(secondWebServer, 404, "template [" + template + "] does not exist");
                    enqueueResponse(secondWebServer, 201, "template [" + template + "] created");
                }
            }
            enqueuePipelineResponses(secondWebServer, !pipelineExistsAlready);
            enqueueResponse(secondWebServer, 200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

            logger.info("--> exporting a second event");
            export(Collections.singletonList(newRandomMonitoringDoc()));

            assertThat(secondWebServer.getRequestCount(), equalTo(2 + monitoringTemplates().size() * 2 - 1 + expectedPipelineCalls));
            assertMonitorVersion(secondWebServer);

            for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
                RecordedRequest recordedRequest = secondWebServer.takeRequest();
                assertThat(recordedRequest.getMethod(), equalTo("GET"));
                assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));

                if (template.getKey().contains(MonitoringBulkTimestampedResolver.Data.DATA) == false) {
                    recordedRequest = secondWebServer.takeRequest();
                    assertThat(recordedRequest.getMethod(), equalTo("PUT"));
                    assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));
                    assertThat(recordedRequest.getBody().readUtf8(), equalTo(template.getValue()));
                }
            }
            assertMonitorPipelines(secondWebServer, !pipelineExistsAlready, null);
            assertBulk(secondWebServer);
        } finally {
            if (secondWebServer != null) {
                secondWebServer.shutdown();
            }
        }
    }

    public void testUnsupportedClusterVersion() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false);

        // returning an unsupported cluster version
        enqueueGetClusterVersionResponse(randomFrom(Version.fromString("0.18.0"), Version.fromString("1.0.0"),
                Version.fromString("1.4.0")));

        String agentNode = internalCluster().startNode(builder);

        HttpExporter exporter = getExporter(agentNode);
        assertThat(exporter.supportedClusterVersion, is(false));
        assertNull(exporter.openBulk());

        assertThat(exporter.supportedClusterVersion, is(false));
        assertThat(webServer.getRequestCount(), equalTo(1));

        assertMonitorVersion(webServer);
    }

    public void testDynamicIndexFormatChange() throws Exception {
        final boolean templatesExistsAlready = randomBoolean();
        final boolean pipelineExistsAlready = randomBoolean();
        final int expectedTemplateAndPipelineCalls = expectedTemplateAndPipelineCalls(templatesExistsAlready, pipelineExistsAlready);

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.exporters._http.update_mappings", false);

        String agentNode = internalCluster().startNode(builder);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueTemplateAndPipelineResponses(webServer, templatesExistsAlready, pipelineExistsAlready);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        HttpExporter exporter = getExporter(agentNode);

        MonitoringDoc doc = newRandomMonitoringDoc();
        export(Collections.singletonList(doc));

        final int expectedRequests = 2 + expectedTemplateAndPipelineCalls;
        assertThat(webServer.getRequestCount(), equalTo(expectedRequests));
        assertMonitorTemplatesAndPipeline(webServer, templatesExistsAlready, pipelineExistsAlready);
        RecordedRequest recordedRequest = assertBulk(webServer);

        String indexName = exporter.getResolvers().getResolver(doc).index(doc);

        byte[] bytes = recordedRequest.getBody().readByteArray();
        Map<String, Object> data = XContentHelper.convertToMap(new BytesArray(bytes), false).v2();
        Map<String, Object> index = (Map<String, Object>) data.get("index");
        assertThat(index.get("_index"), equalTo(indexName));

        String newTimeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("xpack.monitoring.exporters._http.index.name.time_format", newTimeFormat)));

        enqueueGetClusterVersionResponse(Version.CURRENT);
        enqueueTemplateAndPipelineResponses(webServer, true, true);
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        doc = newRandomMonitoringDoc();
        export(Collections.singletonList(doc));

        String expectedMonitoringIndex = ".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(newTimeFormat).withZoneUTC().print(doc.getTimestamp());

        final int expectedTemplatesAndPipelineExists = expectedTemplateAndPipelineCalls(true, true);
        assertThat(webServer.getRequestCount(), equalTo(expectedRequests + 2 + expectedTemplatesAndPipelineExists));
        assertMonitorTemplatesAndPipeline(webServer, true, true);
        recordedRequest = assertBulk(webServer);

        bytes = recordedRequest.getBody().readByteArray();
        data = XContentHelper.convertToMap(new BytesArray(bytes), false).v2();
        index = (Map<String, Object>) data.get("index");
        assertThat(index.get("_index"), equalTo(expectedMonitoringIndex));
    }

    public void testLoadRemoteClusterVersion() throws IOException {
        final String host = webServer.getHostName() + ":" + webServer.getPort();

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", host)
                .put("xpack.monitoring.exporters._http.connection.keep_alive", false);

        String agentNode = internalCluster().startNode(builder);
        HttpExporter exporter = getExporter(agentNode);

        enqueueGetClusterVersionResponse(Version.CURRENT);
        Version resolved = exporter.loadRemoteClusterVersion(host);
        assertTrue(resolved.equals(Version.CURRENT));

        final Version expected = randomFrom(Version.CURRENT, Version.V_2_0_0_beta1, Version.V_2_0_0_beta2, Version.V_2_0_0_rc1,
                Version.V_2_0_0, Version.V_2_1_0, Version.V_2_2_0, Version.V_2_3_0);
        enqueueGetClusterVersionResponse(expected);
        resolved = exporter.loadRemoteClusterVersion(host);
        assertTrue(resolved.equals(expected));
    }

    private void export(Collection<MonitoringDoc> docs) throws Exception {
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        assertThat(exporters, notNullValue());

        // Wait for exporting bulks to be ready to export
        assertBusy(() -> exporters.forEach(exporter -> assertThat(exporter.openBulk(), notNullValue())));
        exporters.export(docs);
    }

    private HttpExporter getExporter(String nodeName) {
        Exporters exporters = internalCluster().getInstance(Exporters.class, nodeName);
        return (HttpExporter) exporters.iterator().next();
    }

    private MonitoringDoc newRandomMonitoringDoc() {
        if (randomBoolean()) {
            IndexRecoveryMonitoringDoc doc = new IndexRecoveryMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));
            doc.setRecoveryResponse(new RecoveryResponse());
            return doc;
        } else {
            ClusterStateMonitoringDoc doc = new ClusterStateMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));
            doc.setClusterState(ClusterState.PROTO);
            doc.setStatus(ClusterHealthStatus.GREEN);
            return doc;
        }
    }

    private List<MonitoringDoc> newRandomMonitoringDocs(int nb) {
        List<MonitoringDoc> docs = new ArrayList<>(nb);
        for (int i = 0; i < nb; i++) {
            docs.add(newRandomMonitoringDoc());
        }
        return docs;
    }

    private void enqueueGetClusterVersionResponse(Version v) throws IOException {
        enqueueGetClusterVersionResponse(webServer, v);
    }

    private void enqueueGetClusterVersionResponse(MockWebServer mockWebServer, Version v) throws IOException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                jsonBuilder().startObject().startObject("version").field("number", v.toString()).endObject().endObject().bytes()
                        .utf8ToString()));
    }

    private void enqueueTemplateAndPipelineResponses(final MockWebServer webServer,
                                                     final boolean templatesAlreadyExists, final boolean pipelineAlreadyExists)
            throws IOException {
        enqueueTemplateResponses(webServer, templatesAlreadyExists);
        enqueuePipelineResponses(webServer, pipelineAlreadyExists);
    }

    private void enqueueTemplateResponses(final MockWebServer webServer, final boolean alreadyExists) throws IOException {
        if (alreadyExists) {
            enqueueTemplateResponsesExistsAlready(webServer);
        } else {
            enqueueTemplateResponsesDoesNotExistYet(webServer);
        }
    }

    private void enqueueTemplateResponsesDoesNotExistYet(final MockWebServer webServer) throws IOException {
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(webServer, 404, "template [" + template + "] does not exist");
            enqueueResponse(webServer, 201, "template [" + template + "] created");
        }
    }

    private void enqueueTemplateResponsesExistsAlready(final MockWebServer webServer) throws IOException {
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(webServer, 200, "template [" + template + "] exists");
        }
    }

    private void enqueuePipelineResponses(final MockWebServer webServer, final boolean alreadyExists) throws IOException {
        if (alreadyExists) {
            enqueuePipelineResponsesExistsAlready(webServer);
        } else {
            enqueuePipelineResponsesDoesNotExistYet(webServer);
        }
    }

    private void enqueuePipelineResponsesDoesNotExistYet(final MockWebServer webServer) throws IOException {
        enqueueResponse(webServer, 404, "pipeline [" + Exporter.EXPORT_PIPELINE_NAME + "] does not exist");
        enqueueResponse(webServer, 201, "pipeline [" + Exporter.EXPORT_PIPELINE_NAME + "] created");
    }

    private void enqueuePipelineResponsesExistsAlready(final MockWebServer webServer) throws IOException {
        enqueueResponse(webServer, 200, "pipeline [" + Exporter.EXPORT_PIPELINE_NAME + "] exists");
    }

    private void enqueueResponse(int responseCode, String body) throws IOException {
        enqueueResponse(webServer, responseCode, body);
    }

    private void enqueueResponse(MockWebServer mockWebServer, int responseCode, String body) throws IOException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));
    }

    private void assertBulkRequest(Buffer requestBody, int numberOfActions) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest().add(new BytesArray(requestBody.readByteArray()), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(numberOfActions));
        for (ActionRequest actionRequest : bulkRequest.requests()) {
            assertThat(actionRequest, instanceOf(IndexRequest.class));
        }
    }
}
