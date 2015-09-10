/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMarvelDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Matchers;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;


// Transport Client instantiation also calls the marvel plugin, which then fails to find modules
@ClusterScope(transportClientRatio = 0.0, scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class HttpESExporterTests extends ESIntegTestCase {

    final static AtomicLong timeStampGenerator = new AtomicLong();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LicensePlugin.class, MarvelPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Test
    public void testHttpServerOff() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, false);
        internalCluster().startNode(builder);
        HttpESExporter httpEsExporter = getEsExporter();
        logger.info("trying exporting despite of no target");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));
    }

    @Test
    public void testTemplateAdditionDespiteOfLateClusterForming() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true)
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "1s")
                .put("discovery.initial_state_timeout", "100ms")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(HttpESExporter.SETTINGS_BULK_TIMEOUT, "1s")
                .put(HttpESExporter.SETTINGS_CHECK_TEMPLATE_TIMEOUT, "1s");

        internalCluster().startNode(builder);

        HttpESExporter httpEsExporter = getEsExporter();
        logger.info("exporting events while there is no cluster");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("bringing up a second node");
        internalCluster().startNode(builder);
        ensureGreen();
        logger.info("exporting a second event");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateExists();
    }

    @Test
    public void testDynamicHostChange() {
        // disable exporting to be able to use non valid hosts
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.INTERVAL, "-1");
        internalCluster().startNode(builder);

        HttpESExporter httpEsExporter = getEsExporter();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, "test1")));
        assertThat(httpEsExporter.getHosts(), Matchers.arrayContaining("test1"));

        // wipes the non array settings
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putArray(HttpESExporter.SETTINGS_HOSTS, "test2").put(HttpESExporter.SETTINGS_HOSTS, "")));
        assertThat(httpEsExporter.getHosts(), Matchers.arrayContaining("test2"));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, "test3")));
        assertThat(httpEsExporter.getHosts(), Matchers.arrayContaining("test3"));
    }

    @Test
    public void testHostChangeReChecksTemplate() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true);
        internalCluster().startNode(builder);

        HttpESExporter httpEsExporter = getEsExporter();

        logger.info("exporting an event");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("removing the marvel template");
        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());
        assertMarvelTemplateNotExists();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, httpEsExporter.getHosts())).get());

        logger.info("exporting a second event");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateExists();
    }

    @Test
    public void testHostFailureChecksTemplate() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true);

        final String node0 = internalCluster().startNode(builder);
        final HttpESExporter httpEsExporter0 = getEsExporter(node0);
        assertThat(node0, equalTo(internalCluster().getMasterName()));

        final String node1 = internalCluster().startNode(builder);
        final HttpESExporter httpEsExporter1 = getEsExporter(node1);

        logger.info("--> exporting events to force host resolution");
        httpEsExporter0.export(Collections.singletonList(newRandomMarvelDoc()));
        httpEsExporter1.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("--> setting exporting hosts to {} + {}", httpEsExporter0.getHosts(), httpEsExporter1.getHosts());
        ArrayList<String> mergedHosts = new ArrayList<String>();
        mergedHosts.addAll(Arrays.asList(httpEsExporter0.getHosts()));
        mergedHosts.addAll(Arrays.asList(httpEsExporter1.getHosts()));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, mergedHosts.toArray(Strings.EMPTY_ARRAY))).get());

        logger.info("--> exporting events to have new settings take effect");
        httpEsExporter0.export(Collections.singletonList(newRandomMarvelDoc()));
        httpEsExporter1.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("verifying that template has been created");
        assertMarvelTemplateExists();

        logger.info("--> removing the marvel template");
        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());
        assertMarvelTemplateNotExists();

        logger.info("--> shutting down node0");
        internalCluster().stopCurrentMasterNode();

        logger.info("--> exporting events from node1");
        // we use assert busy node because url caching may cause the node failure to be only detected while sending the event
        assertBusy(new Runnable() {
            @Override
            public void run() {
                httpEsExporter1.export(Collections.singletonList(newRandomMarvelDoc()));
                logger.debug("--> checking for template");
                assertMarvelTemplateExists();
            }
        });
    }

    @Test
    public void testDynamicIndexFormatChange() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true);
        String nodeId = internalCluster().startNode(builder);

        logger.info("exporting a first event");
        HttpESExporter httpEsExporter = getEsExporter(nodeId);
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        logger.info("checks that the index [{}] is created", httpEsExporter.getIndexName());
        assertTrue(client().admin().indices().prepareExists(httpEsExporter.getIndexName()).get().isExists());

        String newTimeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        logger.info("updating index time format setting to {}", newTimeFormat);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(HttpESExporter.SETTINGS_INDEX_TIME_FORMAT, newTimeFormat)));

        logger.info("exporting a second event");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        String expectedMarvelIndex = MarvelSettings.MARVEL_INDICES_PREFIX
                + DateTimeFormat.forPattern(newTimeFormat).withZoneUTC().print(System.currentTimeMillis());

        logger.info("checks that the index [{}] is created", expectedMarvelIndex);
        assertTrue(client().admin().indices().prepareExists(expectedMarvelIndex).get().isExists());

        logger.info("verifying that template has been created");
        assertMarvelTemplateExists();
    }

    @Test
    public void testLoadRemoteClusterVersion() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true);
        String nodeId = internalCluster().startNode(builder);

        HttpESExporter httpEsExporter = getEsExporter(nodeId);

        logger.info("--> exporting events to force host resolution");
        httpEsExporter.export(Collections.singletonList(newRandomMarvelDoc()));

        assertNotNull(httpEsExporter.getHosts());
        assertThat(httpEsExporter.getHosts().length, greaterThan(0));

        logger.info("--> loading remote cluster version");
        Version resolved = httpEsExporter.loadRemoteClusterVersion(httpEsExporter.getHosts()[0]);
        assertTrue(resolved.equals(Version.CURRENT));
    }

    private HttpESExporter getEsExporter() {
        AgentService service = internalCluster().getInstance(AgentService.class);
        return (HttpESExporter) service.getExporters().iterator().next();
    }

    private HttpESExporter getEsExporter(String node) {
        AgentService service = internalCluster().getInstance(AgentService.class, node);
        return (HttpESExporter) service.getExporters().iterator().next();
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

    private void assertMarvelTemplateExists() {
        assertTrue("marvel template must exists", isTemplateExists("marvel"));
    }

    private void assertMarvelTemplateNotExists() {
        assertFalse("marvel template must not exists", isTemplateExists("marvel"));
    }

    private boolean isTemplateExists(String templateName) {
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(templateName).get().getIndexTemplates()) {
            if (template.getName().equals(templateName)) {
                return true;
            }
        }
        return false;
    }
}
