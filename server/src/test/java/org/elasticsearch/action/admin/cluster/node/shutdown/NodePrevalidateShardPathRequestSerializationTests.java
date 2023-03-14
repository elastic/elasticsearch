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

import static org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathRequestSerializationTests.createSetMutation;

public class NodePrevalidateShardPathRequestSerializationTests extends AbstractWireSerializingTestCase<NodePrevalidateShardPathRequest> {

    @Override
    protected Writeable.Reader<NodePrevalidateShardPathRequest> instanceReader() {
        return NodePrevalidateShardPathRequest::new;
    }

    @Override
    protected NodePrevalidateShardPathRequest createTestInstance() {
        return new NodePrevalidateShardPathRequest(randomSet(0, 50, PrevalidateShardPathRequestSerializationTests::randomShardId));
    }

    @Override
    protected NodePrevalidateShardPathRequest mutateInstance(NodePrevalidateShardPathRequest request) {
        return new NodePrevalidateShardPathRequest(
            createSetMutation(request.getShardIds(), PrevalidateShardPathRequestSerializationTests::randomShardId)
        );
    }
}
