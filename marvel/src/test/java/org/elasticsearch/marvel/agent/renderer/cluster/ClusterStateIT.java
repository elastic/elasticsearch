/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;

public class ClusterStateIT extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, ClusterStateCollector.NAME)
                .build();
    }

    @Test
    public void testClusterState() throws Exception {
        ensureGreen();

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
     *
     * See
     */
    @Test
    public void testNoNodesIndexing() throws Exception {
        ensureGreen();

        logger.debug("--> forcing marvel's index template update");
        assertAcked(client().admin().indices().preparePutTemplate("marvel").setSource(HttpExporterUtils.loadDefaultTemplate()).execute().actionGet());

        logger.debug("--> deleting all marvel indices");
        deleteMarvelIndices();

        logger.debug("--> checking for template existence");
        awaitMarvelDocsCount(greaterThan(0L), ClusterStateCollector.TYPE);
        assertMarvelTemplateInstalled();

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStateCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStateCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        DiscoveryNodes nodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();

        logger.debug("--> ensure that the 'nodes' attributes of the cluster state document is not indexed");
        assertHitCount(client().prepareCount()
                .setTypes(ClusterStateCollector.TYPE)
                .setQuery(QueryBuilders.matchQuery("cluster_state.nodes." + nodes.masterNodeId() + ".name", nodes.masterNode().name())).get(), 0L);
    }
}
