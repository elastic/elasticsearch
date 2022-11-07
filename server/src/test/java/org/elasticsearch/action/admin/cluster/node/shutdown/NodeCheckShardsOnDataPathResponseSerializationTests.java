/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.action.admin.cluster.node.shutdown.CheckShardsOnDataPathRequestSerializationTests.createSetMutation;

public class NodeCheckShardsOnDataPathResponseSerializationTests extends AbstractWireSerializingTestCase<
    NodeCheckShardsOnDataPathResponse> {

    @Override
    protected Writeable.Reader<NodeCheckShardsOnDataPathResponse> instanceReader() {
        return NodeCheckShardsOnDataPathResponse::new;
    }

    @Override
    protected NodeCheckShardsOnDataPathResponse createTestInstance() {
        return getRandomResponse();
    }

    public static NodeCheckShardsOnDataPathResponse getRandomResponse() {
        return new NodeCheckShardsOnDataPathResponse(
            getRandomNode(),
            randomSet(0, 100, CheckShardsOnDataPathRequestSerializationTests::randomShardId)
        );
    }

    public static DiscoveryNode getRandomNode() {
        return new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT);
    }

    @Override
    protected NodeCheckShardsOnDataPathResponse mutateInstance(NodeCheckShardsOnDataPathResponse response) throws IOException {
        if (randomBoolean()) {
            return new NodeCheckShardsOnDataPathResponse(getRandomNode(), response.getShardIds());
        }
        return new NodeCheckShardsOnDataPathResponse(
            response.getNode(),
            createSetMutation(response.getShardIds(), CheckShardsOnDataPathRequestSerializationTests::randomShardId)
        );
    }
}
