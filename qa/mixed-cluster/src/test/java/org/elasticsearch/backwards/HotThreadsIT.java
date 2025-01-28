/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.hamcrest.Matchers.equalTo;

public class HotThreadsIT extends ESRestTestCase {

    private static final String BWC_NODES_VERSION = System.getProperty("tests.bwc_nodes_version");

    public void testHotThreads() throws Exception {
        final MixedClusterTestNodes nodes = MixedClusterTestNodes.buildNodes(client(), BWC_NODES_VERSION);
        assumeFalse("no new node found", nodes.getNewNodes().isEmpty());
        assumeFalse("no bwc node found", nodes.getBWCNodes().isEmpty());
        assumeTrue(
            "new nodes are higher version than BWC nodes",
            nodes.getNewNodes().get(0).version().compareTo(nodes.getBWCNodes().get(0).version()) > 0
        );
        final Request request = new Request("GET", "/_nodes/hot_threads");
        final Response response = client().performRequest(request);
        final String responseString = EntityUtils.toString(response.getEntity());
        final String[] nodeResponses = responseString.split("::: ");
        int respondedNodes = 0;
        for (String nodeResponse : nodeResponses) {
            final String[] lines = nodeResponse.split("\n");
            final String nodeId = lines[0].trim();
            if (nodeId.isEmpty() == false) {
                respondedNodes++;
            }
        }
        assertThat(respondedNodes, equalTo(nodes.getNewNodes().size() + nodes.getBWCNodes().size()));
    }
}
