/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@ClusterScope(scope = Scope.TEST)
public class ClusterStateTests extends MarvelIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "-1")
                .put(MarvelSettings.COLLECTORS, ClusterStateCollector.NAME)
                .put("marvel.agent.exporters.default_local.type", "local")
                .put("marvel.agent.exporters.default_local.template.settings.index.number_of_replicas", 0)
                .build();
    }

    @Before
    public void init() throws Exception {
        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testClusterState() throws Exception {
        logger.debug("--> waiting for documents to be collected");
        awaitMarvelDocsCount(greaterThan(0L), ClusterStateCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStateCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = ClusterStateRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> cluster state successfully collected");
    }

    /**
     * This test should fail if the mapping for the 'nodes' attribute
     * in the 'cluster_state' document is NOT set to 'enable: false'
     */
    public void testNoNodesIndexing() throws Exception {
        logger.debug("--> waiting for documents to be collected");
        awaitMarvelDocsCount(greaterThan(0L), ClusterStateCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStateCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        DiscoveryNodes nodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();

        logger.debug("--> ensure that the 'nodes' attributes of the cluster state document is not indexed");
        assertHitCount(client().prepareSearch().setSize(0)
                .setTypes(ClusterStateCollector.TYPE)
                .setQuery(QueryBuilders.matchQuery("cluster_state.nodes." + nodes.masterNodeId() + ".name", nodes.masterNode().name())).get(), 0L);
    }

    public void testClusterStateNodes() throws Exception {
        final long nbNodes = internalCluster().size();

        logger.debug("--> waiting for documents to be collected");
        awaitMarvelDocsCount(greaterThanOrEqualTo(nbNodes), ClusterStateCollector.NODES_TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStateCollector.NODES_TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateCollector.NODES_TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThanOrEqualTo(nbNodes));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = {
                AbstractRenderer.Fields.CLUSTER_UUID.underscore().toString(),
                AbstractRenderer.Fields.TIMESTAMP.underscore().toString(),
                ClusterStateNodeRenderer.Fields.STATE_UUID.underscore().toString(),
                ClusterStateNodeRenderer.Fields.NODE.underscore().toString(),
                ClusterStateNodeRenderer.Fields.NODE.underscore().toString() + "." + ClusterStateNodeRenderer.Fields.ID.underscore().toString(),
        };

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> cluster state nodes successfully collected");
    }

    public void testClusterStateNode() throws Exception {
        final long nbNodes = internalCluster().size();

        logger.debug("--> waiting for documents to be collected");
        awaitMarvelDocsCount(greaterThanOrEqualTo(nbNodes), ClusterStateCollector.NODE_TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStateCollector.NODE_TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateCollector.NODE_TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThanOrEqualTo(nbNodes));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = {
                AbstractRenderer.Fields.CLUSTER_UUID.underscore().toString(),
                AbstractRenderer.Fields.TIMESTAMP.underscore().toString(),
                DiscoveryNodeRenderer.Fields.NODE.underscore().toString(),
                DiscoveryNodeRenderer.Fields.NODE.underscore().toString() + "." + DiscoveryNodeRenderer.Fields.ID.underscore().toString(),
                DiscoveryNodeRenderer.Fields.NODE.underscore().toString() + "." + DiscoveryNodeRenderer.Fields.NAME.underscore().toString(),
                DiscoveryNodeRenderer.Fields.NODE.underscore().toString() + "." + DiscoveryNodeRenderer.Fields.ATTRIBUTES.underscore().toString(),
                DiscoveryNodeRenderer.Fields.NODE.underscore().toString() + "." + DiscoveryNodeRenderer.Fields.TRANSPORT_ADDRESS.underscore().toString(),
        };

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        for (final String nodeName : internalCluster().getNodeNames()) {
            final String nodeId = internalCluster().clusterService(nodeName).localNode().getId();

            logger.debug("--> getting marvel document for node id [{}]", nodeId);
            assertThat(client().prepareGet(MarvelSettings.MARVEL_DATA_INDEX_NAME, ClusterStateCollector.NODE_TYPE, nodeId).get().isExists(), is(true));

            // checks that document is not indexed
            assertHitCount(client().prepareSearch().setSize(0)
                    .setTypes(ClusterStateCollector.NODE_TYPE)
                    .setQuery(QueryBuilders.boolQuery()
                            .should(QueryBuilders.matchQuery("node.id", nodeId))
                            .should(QueryBuilders.matchQuery("node.name", nodeName))).get(), 0);
        }

        logger.debug("--> cluster state node successfully collected");
    }
}
