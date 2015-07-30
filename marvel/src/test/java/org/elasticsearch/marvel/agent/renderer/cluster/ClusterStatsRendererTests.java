/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

public class ClusterStatsRendererTests extends ElasticsearchSingleNodeTest {

    private static final String SAMPLE_FILE = "/samples/marvel_cluster_stats.json";

    @Test
    public void testClusterStatsRenderer() throws Exception {
        createIndex("index-0");

        logger.debug("--> retrieving cluster stats response");
        ClusterStatsResponse clusterStats = client().admin().cluster().prepareClusterStats().get();

        logger.debug("--> creating the cluster stats marvel document");
        ClusterStatsMarvelDoc marvelDoc = ClusterStatsMarvelDoc.createMarvelDoc("test", "marvel_cluster_stats", 1437580442979L, clusterStats);

        logger.debug("--> rendering the document");
        Renderer renderer = new ClusterStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = Streams.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }
}
