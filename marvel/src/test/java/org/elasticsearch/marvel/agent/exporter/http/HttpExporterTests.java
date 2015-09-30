/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMarvelDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMarvelDoc;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.SuppressLocalMode;
import org.elasticsearch.test.InternalTestCluster;
import org.hamcrest.Matchers;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;


// Transport Client instantiation also calls the marvel plugin, which then fails to find modules
@SuppressLocalMode
@ClusterScope(scope = TEST, transportClientRatio = 0.0, numDataNodes = 0, numClientNodes = 0)
@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/729")
public class HttpExporterTests extends MarvelIntegTestCase {

    final static AtomicLong timeStampGenerator = new AtomicLong();

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Before
    public void init() throws Exception {
        startCollection();
    }

    @After
    public void cleanup() throws Exception {
        stopCollection();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put("shield.enabled", false)
                .build();
    }

    @Test
    public void testSimpleExport() throws Exception {
        TargetNode target = TargetNode.start(internalCluster());

        Settings.Builder builder = Settings.builder()
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", target.httpAddress);
        String agentNode = internalCluster().startNode(builder);
        ensureGreen();
        HttpExporter exporter = getExporter(agentNode);
        MarvelDoc doc = newRandomMarvelDoc();
        exporter.export(Collections.singletonList(doc));

        flush();
        refresh();

        SearchResponse response = client().prepareSearch(".marvel-es-*").setTypes(doc.type()).get();
        assertThat(response, notNullValue());
        assertThat(response.getHits().totalHits(), is(1L));
    }

    @Test
    public void testTemplateAdditionDespiteOfLateClusterForming() throws Exception {

        TargetNode target = TargetNode.start(internalCluster());

        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true)
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "1s")
                .put("discovery.initial_state_timeout", "100ms")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", target.httpAddress)
                .put("marvel.agent.exporters._http." + HttpExporter.BULK_TIMEOUT_SETTING, "1s")
                .put("marvel.agent.exporters._http." + HttpExporter.TEMPLATE_CHECK_TIMEOUT_SETTING, "1s");

        String nodeName = internalCluster().startNode(builder);

        HttpExporter exporter = getExporter(nodeName);
        logger.info("exporting events while there is no cluster");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("bringing up a second node");
        internalCluster().startNode(builder);
        ensureGreen();
        logger.info("exporting a second event");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateInstalled();
    }

    @Test
    public void testDynamicHostChange() {

        // disable exporting to be able to use non valid hosts
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.INTERVAL, "-1")
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", "test0");

        String nodeName = internalCluster().startNode(builder);

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("marvel.agent.exporters._http.host", "test1")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test1"));

        // wipes the non array settings
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("marvel.agent.exporters._http.host", "test2")
                .put("marvel.agent.exporters._http.host", "")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test2"));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray("marvel.agent.exporters._http.host", "test3")));
        assertThat(getExporter(nodeName).hosts, Matchers.arrayContaining("test3"));
    }

    @Test
    public void testHostChangeReChecksTemplate() throws Exception {

        TargetNode targetNode = TargetNode.start(internalCluster());

        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", targetNode.httpAddress);

        String agentNode = internalCluster().startNode(builder);

        HttpExporter exporter = getExporter(agentNode);

        logger.info("exporting an event");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("removing the marvel template");
        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());
        assertMarvelTemplateMissing();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().putArray("marvel.agent.exporters._http.host", exporter.hosts)).get());

        // a new exporter is created on update, so we need to re-fetch it
        exporter = getExporter(agentNode);

        logger.info("exporting a second event");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateInstalled();
    }

    @Test
    public void testHostFailureChecksTemplate() throws Exception {

        TargetNode target0 = TargetNode.start(internalCluster());
        assertThat(target0.name, is(internalCluster().getMasterName()));

        TargetNode target1 = TargetNode.start(internalCluster());

        // lets start node0 & node1 first, such that node0 will be the master (it's first to start)
        final String node0 = internalCluster().startNode(Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put("marvel.agent.exporters._http.type", "http")
                .putArray("marvel.agent.exporters._http.host", target0.httpAddress, target1.httpAddress));

        HttpExporter exporter = getExporter(node0);

        logger.info("--> exporting events to have new settings take effect");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateInstalled();

        logger.info("--> removing the marvel template");
        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());
        assertMarvelTemplateMissing();

        logger.info("--> shutting down target0");
        assertThat(target0.name, is(internalCluster().getMasterName())); // just to be sure it's still the master
        internalCluster().stopCurrentMasterNode();

        // we use assert busy node because url caching may cause the node failure to be only detected while sending the event
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("--> exporting events from node0");
                    getExporter(node0).export(Collections.singletonList(newRandomMarvelDoc()));
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("failed to export event from node0");
                }
                logger.debug("--> checking for template");
                assertMarvelTemplateInstalled();
                logger.debug("--> template exists");
            }
        }, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testDynamicIndexFormatChange() throws Exception {

        TargetNode targetNode = TargetNode.start(internalCluster());

        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", targetNode.httpAddress);

        String agentNode = internalCluster().startNode(builder);

        logger.info("exporting a first event");
        HttpExporter exporter = getExporter(agentNode);
        MarvelDoc doc = newRandomMarvelDoc();
        exporter.export(Collections.singletonList(doc));

        String indexName = exporter.indexNameResolver().resolve(doc);
        logger.info("checks that the index [{}] is created", indexName);
        assertTrue(client().admin().indices().prepareExists(indexName).get().isExists());

        String newTimeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        logger.info("updating index time format setting to {}", newTimeFormat);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("marvel.agent.exporters._http.index.name.time_format", newTimeFormat)));

        exporter = getExporter(agentNode);

        logger.info("exporting a second event");
        doc = newRandomMarvelDoc();
        exporter.export(Collections.singletonList(doc));

        String expectedMarvelIndex = MarvelSettings.MARVEL_INDICES_PREFIX
                + DateTimeFormat.forPattern(newTimeFormat).withZoneUTC().print(doc.timestamp());

        logger.info("checks that the index [{}] is created", expectedMarvelIndex);
        assertTrue(client().admin().indices().prepareExists(expectedMarvelIndex).get().isExists());

        logger.info("verifying that template has been created");
        assertMarvelTemplateInstalled();
    }

    @Test
    public void testLoadRemoteClusterVersion() {

        TargetNode targetNode = TargetNode.start(internalCluster());

        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put("marvel.agent.exporters._http.type", "http")
                .put("marvel.agent.exporters._http.host", targetNode.httpAddress);

        String agentNode = internalCluster().startNode(builder);

        HttpExporter exporter = getExporter(agentNode);

        logger.info("--> loading remote cluster version");
        Version resolved = exporter.loadRemoteClusterVersion(targetNode.httpAddress);
        assertTrue(resolved.equals(Version.CURRENT));
    }

    private HttpExporter getExporter(String nodeName) {
        Exporters exporters = internalCluster().getInstance(Exporters.class, nodeName);
        return (HttpExporter) exporters.iterator().next();
    }

    private MarvelDoc newRandomMarvelDoc() {
        if (randomBoolean()) {
            return new IndexRecoveryMarvelDoc(internalCluster().getClusterName(),
                    IndexRecoveryCollector.TYPE, timeStampGenerator.incrementAndGet(), new RecoveryResponse());
        } else {
            return new ClusterStateMarvelDoc(internalCluster().getClusterName(),
                    ClusterStateCollector.TYPE, timeStampGenerator.incrementAndGet(), ClusterState.PROTO, ClusterHealthStatus.GREEN);
        }
    }

    static class TargetNode {

        private final String name;
        private final TransportAddress address;
        private final String httpAddress;
        private final Client client;

        private TargetNode(InternalTestCluster cluster) {
            name = cluster.startNode(Settings.builder().put(Node.HTTP_ENABLED, true));
            address = cluster.getInstance(HttpServerTransport.class, name).boundAddress().publishAddress();
            httpAddress = address.getHost() + ":" + address.getPort();
            this.client = cluster.client(name);
        }

        static TargetNode start(InternalTestCluster cluster) {
            return new TargetNode(cluster);
        }
    }
}
