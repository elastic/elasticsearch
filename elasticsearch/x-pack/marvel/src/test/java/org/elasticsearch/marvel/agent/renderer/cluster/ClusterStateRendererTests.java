/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.StreamsUtils;

public class ClusterStateRendererTests extends ESSingleNodeTestCase {
    private static final String SAMPLE_FILE = "/samples/cluster_state.json";

    public void testClusterStateRenderer() throws Exception {
        createIndex("my-index", Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build());

        logger.debug("--> retrieving cluster state");
        ClusterState clusterState = getInstanceFromNode(ClusterService.class).state();

        logger.debug("--> retrieving cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().get();

        logger.debug("--> creating the cluster state monitoring document");
        ClusterStateMarvelDoc marvelDoc = new ClusterStateMarvelDoc();
        marvelDoc.setClusterUUID("test");
        marvelDoc.setType("cluster_state");
        marvelDoc.setTimestamp(1437580442979L);
        marvelDoc.setClusterState(clusterState);
        marvelDoc.setStatus(clusterHealth.getStatus());

        logger.debug("--> rendering the document");
        Renderer renderer = new ClusterStateRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        String nodeId = clusterState.getNodes().getLocalNodeId();
        logger.debug("--> replace the local node id in sample document with {}", nodeId);
        expected = Strings.replace(expected, "__node_id__", nodeId);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }
}
