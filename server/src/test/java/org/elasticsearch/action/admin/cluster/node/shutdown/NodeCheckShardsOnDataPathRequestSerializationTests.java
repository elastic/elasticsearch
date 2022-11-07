/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.action.admin.cluster.node.shutdown.CheckShardsOnDataPathRequestSerializationTests.createSetMutation;

public class NodeCheckShardsOnDataPathRequestSerializationTests extends AbstractWireSerializingTestCase<NodeCheckShardsOnDataPathRequest> {

    @Override
    protected Writeable.Reader<NodeCheckShardsOnDataPathRequest> instanceReader() {
        return NodeCheckShardsOnDataPathRequest::new;
    }

    @Override
    protected NodeCheckShardsOnDataPathRequest createTestInstance() {
        return new NodeCheckShardsOnDataPathRequest(randomSet(0, 50, CheckShardsOnDataPathRequestSerializationTests::randomShardId));
    }

    @Override
    protected NodeCheckShardsOnDataPathRequest mutateInstance(NodeCheckShardsOnDataPathRequest request) throws IOException {
        return new NodeCheckShardsOnDataPathRequest(
            createSetMutation(request.getShardIDs(), CheckShardsOnDataPathRequestSerializationTests::randomShardId)
        );
    }
}
