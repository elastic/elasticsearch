/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMarvelDoc;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Map;

public class NodeStatsRendererTests extends ESSingleNodeTestCase {

    public void testNodeStatsRenderer() throws Exception {
        createIndex("index-0");

        logger.debug("--> retrieving node stats");
        NodeStats nodeStats = getInstanceFromNode(NodeService.class).stats();

        logger.debug("--> creating the node stats marvel document");
        NodeStatsMarvelDoc marvelDoc = new NodeStatsMarvelDoc();
        marvelDoc.setClusterUUID("test");
        marvelDoc.setType("node_stats");
        marvelDoc.setTimestamp(1437580442979L);
        marvelDoc.setNodeId("node-0");
        marvelDoc.setNodeMaster(true);
        marvelDoc.setNodeStats(nodeStats);
        marvelDoc.setMlockall(false);
        marvelDoc.setDiskThresholdWaterMarkHigh(90.0);
        marvelDoc.setDiskThresholdDeciderEnabled(true);

        logger.debug("--> rendering the document");
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            new NodeStatsRenderer().render(marvelDoc, XContentType.JSON, os);
            Map<String, Object> result =  XContentHelper.convertToMap(os.bytes(), false).v2();

            for (String field : NodeStatsRenderer.FILTERS) {
                if (Constants.WINDOWS) {
                    // load average is unavailable on Windows
                    if ("node_stats.os.cpu.load_average.1m".equals(field)) {
                        continue;
                    }
                }
                assertNotNull("expecting field to be present:" + field, XContentMapValues.extractValue(field, result));
            }
        }
    }
}