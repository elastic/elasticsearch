/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NodeInfoRequestTests extends ESTestCase {

    public void testWriteTo() throws Exception {
        final String nodeId = randomAlphaOfLength(8);
        NodesInfoRequest request = new NodesInfoRequest(nodeId);
        randomSubsetOf(NodesInfoMetrics.Metric.allMetrics()).forEach(request::addMetric);
        request.setConcreteNodes(new DiscoveryNode[] { DiscoveryNodeUtils.create(nodeId) });
        final TransportNodesInfoAction.NodeInfoRequest nodeInfoRequest = new TransportNodesInfoAction.NodeInfoRequest(request);
        final TransportNodesInfoAction.NodeInfoRequest deserializedRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeInfoRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new TransportNodesInfoAction.NodeInfoRequest(in);
            }
        }
        assertThat(deserializedRequest, notNullValue());
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.request.requestedMetrics()));
        assertThat(deserializedRequest.request.nodesIds(), emptyArray());
        assertThat(deserializedRequest.request.concreteNodes(), nullValue());
    }
}
