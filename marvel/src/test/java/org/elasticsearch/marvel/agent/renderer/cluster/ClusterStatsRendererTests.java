/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.StreamsUtils;

public class ClusterStatsRendererTests extends ESSingleNodeTestCase {
    private static final String SAMPLE_FILE = "/samples/cluster_stats.json";

    public void testClusterStatsRenderer() throws Exception {
        createIndex("index-0");

        logger.debug("--> retrieving cluster stats response");
        ClusterStatsResponse clusterStats = client().admin().cluster().prepareClusterStats().get();

        logger.debug("--> creating the cluster stats marvel document");
        ClusterStatsMarvelDoc marvelDoc = new ClusterStatsMarvelDoc("test", "cluster_stats", 1437580442979L, clusterStats);

        logger.debug("--> rendering the document");
        Renderer renderer = new ClusterStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }
}
