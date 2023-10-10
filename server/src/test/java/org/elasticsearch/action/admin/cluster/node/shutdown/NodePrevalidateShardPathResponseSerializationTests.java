/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathRequestSerializationTests.createSetMutation;

public class NodePrevalidateShardPathResponseSerializationTests extends AbstractWireSerializingTestCase<NodePrevalidateShardPathResponse> {

    @Override
    protected Writeable.Reader<NodePrevalidateShardPathResponse> instanceReader() {
        return NodePrevalidateShardPathResponse::new;
    }

    @Override
    protected NodePrevalidateShardPathResponse createTestInstance() {
        return getRandomResponse();
    }

    public static NodePrevalidateShardPathResponse getRandomResponse() {
        return new NodePrevalidateShardPathResponse(
            getRandomNode(),
            randomSet(0, 100, PrevalidateShardPathRequestSerializationTests::randomShardId)
        );
    }

    public static DiscoveryNode getRandomNode() {
        return DiscoveryNodeUtils.create(randomAlphaOfLength(10));
    }

    @Override
    protected NodePrevalidateShardPathResponse mutateInstance(NodePrevalidateShardPathResponse response) {
        if (randomBoolean()) {
            return new NodePrevalidateShardPathResponse(getRandomNode(), response.getShardIds());
        }
        return new NodePrevalidateShardPathResponse(
            response.getNode(),
            createSetMutation(response.getShardIds(), PrevalidateShardPathRequestSerializationTests::randomShardId)
        );
    }
}
