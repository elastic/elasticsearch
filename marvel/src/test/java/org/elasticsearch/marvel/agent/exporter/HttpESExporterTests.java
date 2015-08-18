/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;


// Transport Client instantiation also calls the marvel plugin, which then fails to find modules
@ClusterScope(transportClientRatio = 0.0, scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class HttpESExporterTests extends ESIntegTestCase {

    final static AtomicLong timeStampGenerator = new AtomicLong();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", MarvelPlugin.class.getName() + "," + LicensePlugin.class.getName())
                .build();
    }
    
    @Test
    public void testHttpServerOff() {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, false);
        internalCluster().startNode(builder);
        HttpESExporter httpEsExporter = getEsExporter();
        logger.info("trying exporting despite of no target");
        httpEsExporter.export(ImmutableList.of(newRandomMarvelDoc()));
    }

/*
    @Test
    public void testLargeClusterStateSerialization() throws InterruptedException {
        // make sure no other exporting is done (quicker)..
        internalCluster().startNode(Settings.builder().put(AgentService.SETTINGS_INTERVAL, "200m").put(Node.HTTP_ENABLED, true));
        ESExporter esExporter = internalCluster().getInstance(ESExporter.class);
        DiscoveryNodes.Builder nodesBuilder = new DiscoveryNodes.Builder();
        int nodeCount = randomIntBetween(10, 200);
        for (int i = 0; i < nodeCount; i++) {
            nodesBuilder.put(new DiscoveryNode("node_" + i, new LocalTransportAddress("node_" + i), Version.CURRENT));
        }

        // get the current cluster state rather then construct one because the constructors have changed across ES versions
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        ClusterState state = ClusterState.builder(clusterService.state()).nodes(nodesBuilder).build();
        logger.info("exporting cluster state with {} nodes", state.nodes().size());
        esExporter.exportEvents(new Event[]{
                new ClusterEvent.ClusterStateChange(1234l, state, "test", ClusterHealthStatus.GREEN, "testing_1234", "test_source_unique")
        });
        logger.info("done exporting");

        ensureYellow();
        client().admin().indices().prepareRefresh(".marvel-*").get();
        assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("event_source", "test_source_unique")).get(), 1);
    }
    */

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
        httpEsExporter.export(ImmutableList.of(newRandomMarvelDoc()));

        logger.info("bringing up a second node");
        internalCluster().startNode(builder);
        ensureGreen();
        logger.info("exporting a second event");
        httpEsExporter.export(ImmutableList.of(newRandomMarvelDoc()));

        logger.info("verifying template is inserted");
        assertMarvelTemplate();
    }

    @Test
    public void testDynamicHostChange() {
        // disable exporting to be able to use non valid hosts
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.INTERVAL, "-1");
        internalCluster().startNode(builder);

        HttpESExporter httpEsExporter = getEsExporter();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(HttpESExporter.SETTINGS_HOSTS, "test1")));
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
        httpEsExporter.export(ImmutableList.of(newRandomMarvelDoc()));

        logger.info("removing the marvel template");

        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, httpEsExporter.getHosts())).get());

        logger.info("exporting a second event");
        httpEsExporter.export(ImmutableList.of(newRandomMarvelDoc()));

        logger.info("verifying template is inserted");
        assertMarvelTemplate();
    }

    @Test
    public void testHostFailureChecksTemplate() throws InterruptedException, IOException {
        Settings.Builder builder = Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, "200m")
                .put(Node.HTTP_ENABLED, true);
        final String node0 = internalCluster().startNode(builder);
        String node1 = internalCluster().startNode(builder);

        HttpESExporter httpEsExporter0 = getEsExporter(node0);
        final HttpESExporter httpEsExporter1 = getEsExporter(node1);

        logger.info("--> exporting events to force host resolution");
        httpEsExporter0.export(ImmutableList.of(newRandomMarvelDoc()));
        httpEsExporter1.export(ImmutableList.of(newRandomMarvelDoc()));

        logger.info("--> setting exporting hosts to {} + {}", httpEsExporter0.getHosts(), httpEsExporter1.getHosts());
        ArrayList<String> mergedHosts = new ArrayList<String>();
        mergedHosts.addAll(Arrays.asList(httpEsExporter0.getHosts()));
        mergedHosts.addAll(Arrays.asList(httpEsExporter1.getHosts()));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().putArray(HttpESExporter.SETTINGS_HOSTS, mergedHosts.toArray(Strings.EMPTY_ARRAY))).get());

        logger.info("--> exporting events to have new settings take effect");
        httpEsExporter0.export(ImmutableList.of(newRandomMarvelDoc()));
        httpEsExporter1.export(ImmutableList.of(newRandomMarvelDoc()));

        assertMarvelTemplate();

        logger.info("--> removing the marvel template");
        assertAcked(client().admin().indices().prepareDeleteTemplate("marvel").get());

        logger.info("--> shutting down node0");
        internalCluster().stopRandomNode(new Predicate<Settings>() {
            @Override
            public boolean apply(Settings settings) {
                return settings.get("name").equals(node0);
            }
        });

        logger.info("--> exporting events from node1");
        // we use assert busy node because url caching may cause the node failure to be only detected while sending the event
        assertTrue("failed to find a template named 'marvel'", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                httpEsExporter1.export(ImmutableList.of(newRandomMarvelDoc()));
                logger.debug("--> checking for template");
                return findMarvelTemplate();
            }
        }));
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
        return IndexStatsMarvelDoc.createMarvelDoc(internalCluster().getClusterName(), "test_marvelDoc", timeStampGenerator.incrementAndGet(),
                new IndexStats("test_index", null));
    }

    private void assertMarvelTemplate() {
        boolean found;
        found = findMarvelTemplate();
        assertTrue("failed to find a template named `marvel`", found);
    }

    private boolean findMarvelTemplate() {
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates("marvel").get().getIndexTemplates()) {
            if (template.getName().equals("marvel")) {
                return true;
            }
        }
        return false;
    }
}
