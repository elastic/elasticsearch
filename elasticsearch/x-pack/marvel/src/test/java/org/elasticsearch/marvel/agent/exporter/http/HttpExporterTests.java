/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.bulk.MonitoringBulkResolver;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.Matchers;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class HttpExporterTests extends MarvelIntegTestCase {

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

    public void testExport() throws Exception {
        enqueueGetClusterVersionResponse(Version.CURRENT);
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(404, "template [" + template + "] does not exist");
            enqueueResponse(201, "template [" + template + "] created");
        }
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.agent.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.agent.exporters._http.update_mappings", false);

        internalCluster().startNode(builder);

        final int nbDocs = randomIntBetween(1, 25);
        export(newRandomMarvelDocs(nbDocs));

        assertThat(webServer.getRequestCount(), equalTo(2 + monitoringTemplates().size() * 2));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("GET"));
        assertThat(recordedRequest.getPath(), equalTo("/"));

        for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("GET"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));

            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("PUT"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));
            assertThat(recordedRequest.getBody().readUtf8(), equalTo(template.getValue()));
        }

        recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("POST"));
        assertThat(recordedRequest.getPath(), equalTo("/_bulk"));

        assertBulkRequest(recordedRequest.getBody(), nbDocs);
    }

    public void testDynamicHostChange() {
        // disable exporting to be able to use non valid hosts
        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", "test0");

        String nodeName = internalCluster().startNode(builder);

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.agent.exporters._http.host", "test1")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test1"));

        // wipes the non array settings
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.agent.exporters._http.host", "test2")
                .put("xpack.monitoring.agent.exporters._http.host", "")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test2"));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("xpack.monitoring.agent.exporters._http.host", "test3")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test3"));
    }

    public void testHostChangeReChecksTemplate() throws Exception {

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.agent.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.agent.exporters._http.update_mappings", false);

        logger.info("--> starting node");

        enqueueGetClusterVersionResponse(Version.CURRENT);
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(404, "template [" + template + "] does not exist");
            enqueueResponse(201, "template [" + template + "] created");
        }
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        String agentNode = internalCluster().startNode(builder);

        logger.info("--> exporting data");
        HttpExporter exporter = getExporter(agentNode);
        assertThat(exporter.supportedClusterVersion, is(false));
        export(Collections.singletonList(newRandomMarvelDoc()));

        assertThat(exporter.supportedClusterVersion, is(true));
        assertThat(webServer.getRequestCount(), equalTo(2 + monitoringTemplates().size() * 2));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("GET"));
        assertThat(recordedRequest.getPath(), equalTo("/"));

        for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("GET"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));

            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("PUT"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));
            assertThat(recordedRequest.getBody().readUtf8(), equalTo(template.getValue()));
        }

        recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("POST"));
        assertThat(recordedRequest.getPath(), equalTo("/_bulk"));

        logger.info("--> setting up another web server");
        MockWebServer secondWebServer = null;
        int secondWebPort;

        try {
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
                    Settings.builder().putArray("xpack.monitoring.agent.exporters._http.host",
                            secondWebServer.getHostName() + ":" + secondWebServer.getPort())).get());

            // a new exporter is created on update, so we need to re-fetch it
            exporter = getExporter(agentNode);

            enqueueGetClusterVersionResponse(secondWebServer, Version.CURRENT);
            for (String template : monitoringTemplates().keySet()) {
                if (template.contains(MonitoringBulkResolver.Data.DATA)) {
                    enqueueResponse(secondWebServer, 200, "template [" + template + "] exist");
                } else {
                    enqueueResponse(secondWebServer, 404, "template [" + template + "] does not exist");
                    enqueueResponse(secondWebServer, 201, "template [" + template + "] created");
                }
            }
            enqueueResponse(secondWebServer, 200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

            logger.info("--> exporting a second event");
            export(Collections.singletonList(newRandomMarvelDoc()));

            assertThat(secondWebServer.getRequestCount(), equalTo(2 + monitoringTemplates().size() * 2 - 1));

            recordedRequest = secondWebServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("GET"));
            assertThat(recordedRequest.getPath(), equalTo("/"));

            for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
                recordedRequest = secondWebServer.takeRequest();
                assertThat(recordedRequest.getMethod(), equalTo("GET"));
                assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));

                if (template.getKey().contains(MonitoringBulkResolver.Data.DATA) == false) {
                    recordedRequest = secondWebServer.takeRequest();
                    assertThat(recordedRequest.getMethod(), equalTo("PUT"));
                    assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));
                    assertThat(recordedRequest.getBody().readUtf8(), equalTo(template.getValue()));
                }
            }

            recordedRequest = secondWebServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("POST"));
            assertThat(recordedRequest.getPath(), equalTo("/_bulk"));

        } finally {
            if (secondWebServer != null) {
                secondWebServer.shutdown();
            }
        }
    }

    public void testUnsupportedClusterVersion() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.agent.exporters._http.connection.keep_alive", false);

        logger.info("--> starting node");

        // returning an unsupported cluster version
        enqueueGetClusterVersionResponse(randomFrom(Version.fromString("0.18.0"), Version.fromString("1.0.0"),
                Version.fromString("1.4.0")));

        String agentNode = internalCluster().startNode(builder);

        logger.info("--> exporting data");
        HttpExporter exporter = getExporter(agentNode);
        assertThat(exporter.supportedClusterVersion, is(false));
        assertNull(exporter.openBulk());

        assertThat(exporter.supportedClusterVersion, is(false));
        assertThat(webServer.getRequestCount(), equalTo(1));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("GET"));
        assertThat(recordedRequest.getPath(), equalTo("/"));
    }

    public void testDynamicIndexFormatChange() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("xpack.monitoring.agent.exporters._http.connection.keep_alive", false)
                .put("xpack.monitoring.agent.exporters._http.update_mappings", false);

        String agentNode = internalCluster().startNode(builder);

        logger.info("--> exporting a first event");

        enqueueGetClusterVersionResponse(Version.CURRENT);
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(404, "template [" + template + "] does not exist");
            enqueueResponse(201, "template [" + template + "] created");
        }
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        HttpExporter exporter = getExporter(agentNode);

        MonitoringDoc doc = newRandomMarvelDoc();
        export(Collections.singletonList(doc));

        final int expectedRequests = 2 + monitoringTemplates().size() * 2;
        assertThat(webServer.getRequestCount(), equalTo(expectedRequests));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("GET"));
        assertThat(recordedRequest.getPath(), equalTo("/"));

        for (Map.Entry<String, String> template : monitoringTemplates().entrySet()) {
            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("GET"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));

            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("PUT"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template.getKey()));
            assertThat(recordedRequest.getBody().readUtf8(), equalTo(template.getValue()));
        }

        recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("POST"));
        assertThat(recordedRequest.getPath(), equalTo("/_bulk"));

        String indexName = exporter.getResolvers().getResolver(doc).index(doc);
        logger.info("--> checks that the document in the bulk request is indexed in [{}]", indexName);

        byte[] bytes = recordedRequest.getBody().readByteArray();
        Map<String, Object> data = XContentHelper.convertToMap(new BytesArray(bytes), false).v2();
        Map<String, Object> index = (Map<String, Object>) data.get("index");
        assertThat(index.get("_index"), equalTo(indexName));

        String newTimeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        logger.info("--> updating index time format setting to {}", newTimeFormat);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("xpack.monitoring.agent.exporters._http.index.name.time_format", newTimeFormat)));


        logger.info("--> exporting a second event");

        enqueueGetClusterVersionResponse(Version.CURRENT);
        for (String template : monitoringTemplates().keySet()) {
            enqueueResponse(200, "template [" + template + "] exist");
        }
        enqueueResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");

        doc = newRandomMarvelDoc();
        export(Collections.singletonList(doc));

        String expectedMarvelIndex = ".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(newTimeFormat).withZoneUTC().print(doc.getTimestamp());

        assertThat(webServer.getRequestCount(), equalTo(expectedRequests + 2 + monitoringTemplates().size()));

        recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("GET"));
        assertThat(recordedRequest.getPath(), equalTo("/"));

        for (String template : monitoringTemplates().keySet()) {
            recordedRequest = webServer.takeRequest();
            assertThat(recordedRequest.getMethod(), equalTo("GET"));
            assertThat(recordedRequest.getPath(), equalTo("/_template/" + template));
        }

        recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getMethod(), equalTo("POST"));
        assertThat(recordedRequest.getPath(), equalTo("/_bulk"));

        logger.info("--> checks that the document in the bulk request is indexed in [{}]", expectedMarvelIndex);

        bytes = recordedRequest.getBody().readByteArray();
        data = XContentHelper.convertToMap(new BytesArray(bytes), false).v2();
        index = (Map<String, Object>) data.get("index");
        assertThat(index.get("_index"), equalTo(expectedMarvelIndex));
    }

    public void testLoadRemoteClusterVersion() throws IOException {
        final String host = webServer.getHostName() + ":" + webServer.getPort();

        Settings.Builder builder = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.host", host)
                .put("xpack.monitoring.agent.exporters._http.connection.keep_alive", false);

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

    private MonitoringDoc newRandomMarvelDoc() {
        if (randomBoolean()) {
            IndexRecoveryMonitoringDoc doc = new IndexRecoveryMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
            doc.setRecoveryResponse(new RecoveryResponse());
            return doc;
        } else {
            ClusterStateMonitoringDoc doc = new ClusterStateMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
            doc.setClusterState(ClusterState.PROTO);
            doc.setStatus(ClusterHealthStatus.GREEN);
            return doc;
        }
    }

    private List<MonitoringDoc> newRandomMarvelDocs(int nb) {
        List<MonitoringDoc> docs = new ArrayList<>(nb);
        for (int i = 0; i < nb; i++) {
            docs.add(newRandomMarvelDoc());
        }
        return docs;
    }

    private void enqueueGetClusterVersionResponse(Version v) throws IOException {
        enqueueGetClusterVersionResponse(webServer, v);
    }

    private void enqueueGetClusterVersionResponse(MockWebServer mockWebServer, Version v) throws IOException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                jsonBuilder().startObject().startObject("version").field("number", v.toString()).endObject().endObject().bytes().toUtf8()));
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
