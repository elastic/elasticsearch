/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NodesDeprecationCheckResponseTests
    extends AbstractStreamableTestCase<NodesDeprecationCheckResponse> {

    @Override
    protected NodesDeprecationCheckResponse createBlankInstance() {
        return new NodesDeprecationCheckResponse();
    }

    @Override
    protected NodesDeprecationCheckResponse createTestInstance() {

        List<NodesDeprecationCheckAction.NodeResponse> responses =
            Arrays.asList(randomArray(1, 10, NodesDeprecationCheckAction.NodeResponse[]::new,
                NodesDeprecationCheckResponseTests::randomNodeResponse));
        return new NodesDeprecationCheckResponse(new ClusterName(randomAlphaOfLength(10)),
            responses,
            Collections.emptyList());
    }

    private static DiscoveryNode randomDiscoveryNode() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress(randomAlphaOfLength(5),
            new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        return new DiscoveryNode(randomAlphaOfLength(5), randomAlphaOfLength(5), transportAddress,
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
    }

    private static NodesDeprecationCheckAction.NodeResponse randomNodeResponse() {
        DiscoveryNode node;
        try {
            node = randomDiscoveryNode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<DeprecationIssue> issuesList = Arrays.asList(randomArray(0,10, DeprecationIssue[]::new,
            DeprecationIssueTests::createTestInstance));
        return new NodesDeprecationCheckAction.NodeResponse(node, issuesList);
    }
}
