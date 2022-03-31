/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NodesDeprecationCheckResponseTests extends AbstractWireSerializingTestCase<NodesDeprecationCheckResponse> {

    @Override
    protected Writeable.Reader<NodesDeprecationCheckResponse> instanceReader() {
        return NodesDeprecationCheckResponse::new;
    }

    @Override
    protected NodesDeprecationCheckResponse createTestInstance() {

        List<NodesDeprecationCheckAction.NodeResponse> responses = Arrays.asList(
            randomArray(1, 10, NodesDeprecationCheckAction.NodeResponse[]::new, NodesDeprecationCheckResponseTests::randomNodeResponse)
        );
        return new NodesDeprecationCheckResponse(new ClusterName(randomAlphaOfLength(10)), responses, Collections.emptyList());
    }

    @Override
    protected NodesDeprecationCheckResponse mutateInstance(NodesDeprecationCheckResponse instance) throws IOException {
        int mutate = randomIntBetween(1, 3);
        switch (mutate) {
            case 1 -> {
                List<NodesDeprecationCheckAction.NodeResponse> responses = new ArrayList<>(instance.getNodes());
                responses.add(randomNodeResponse());
                return new NodesDeprecationCheckResponse(instance.getClusterName(), responses, instance.failures());
            }
            case 2 -> {
                ArrayList<FailedNodeException> failures = new ArrayList<>(instance.failures());
                failures.add(new FailedNodeException("test node", "test failure", new RuntimeException(randomAlphaOfLength(10))));
                return new NodesDeprecationCheckResponse(instance.getClusterName(), instance.getNodes(), failures);
            }
            case 3 -> {
                String clusterName = randomValueOtherThan(instance.getClusterName().value(), () -> randomAlphaOfLengthBetween(5, 15));
                return new NodesDeprecationCheckResponse(new ClusterName(clusterName), instance.getNodes(), instance.failures());
            }
            default -> fail("invalid mutation");
        }

        return super.mutateInstance(instance);
    }

    private static DiscoveryNode randomDiscoveryNode() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress(
            randomAlphaOfLength(5),
            new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 }
        );
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        return new DiscoveryNode(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            transportAddress,
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
    }

    private static NodesDeprecationCheckAction.NodeResponse randomNodeResponse() {
        DiscoveryNode node;
        try {
            node = randomDiscoveryNode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<DeprecationIssue> issuesList = Arrays.asList(
            randomArray(0, 10, DeprecationIssue[]::new, NodesDeprecationCheckResponseTests::createTestDeprecationIssue)
        );
        return new NodesDeprecationCheckAction.NodeResponse(node, issuesList);
    }

    private static DeprecationIssue createTestDeprecationIssue() {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }
}
