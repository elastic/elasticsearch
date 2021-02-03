/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class NodeAcknowledgedResponseTests extends AbstractWireSerializingTestCase<NodeAcknowledgedResponse> {

    @Override
    protected NodeAcknowledgedResponse createTestInstance() {
        return new NodeAcknowledgedResponse(true, randomFrom(randomAlphaOfLength(10), ""));
    }

    @Override
    protected Writeable.Reader<NodeAcknowledgedResponse> instanceReader() {
        return NodeAcknowledgedResponse::new;
    }

    @Override
    protected NodeAcknowledgedResponse mutateInstance(NodeAcknowledgedResponse instance) {
        if (instance.getNode().isEmpty()) {
            return new NodeAcknowledgedResponse(true, randomAlphaOfLength(10));
        } else {
            return new NodeAcknowledgedResponse(true, "");
        }
    }
}
