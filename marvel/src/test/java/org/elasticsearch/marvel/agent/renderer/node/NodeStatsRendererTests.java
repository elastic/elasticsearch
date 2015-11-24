/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.StreamsUtils;

public class NodeStatsRendererTests extends ESSingleNodeTestCase {
    private static final String SAMPLE_FILE = "/samples/node_stats.json";

    public void testNodeStatsRenderer() throws Exception {
        createIndex("index-0");

        logger.debug("--> retrieving node stats");
        NodeStats nodeStats = getInstanceFromNode(NodeService.class).stats();

        logger.debug("--> creating the node stats marvel document");
        NodeStatsMarvelDoc marvelDoc = new NodeStatsMarvelDoc("test", "node_stats", 1437580442979L,
                "node-0", true, nodeStats, false, 90.0, true);

        logger.debug("--> rendering the document");
        Renderer renderer = new NodeStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }
}