/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class NodesDeprecationCheckRequestTests extends AbstractWireSerializingTestCase<NodesDeprecationCheckRequest> {

    @Override
    protected Writeable.Reader<NodesDeprecationCheckRequest> instanceReader() {
        return NodesDeprecationCheckRequest::new;
    }

    @Override
    protected NodesDeprecationCheckRequest mutateInstance(NodesDeprecationCheckRequest instance) {
        int newSize = randomValueOtherThan(instance.nodesIds().length, () -> randomIntBetween(0, 10));
        String[] newNodeIds = randomArray(newSize, newSize, String[]::new, () -> randomAlphaOfLengthBetween(5, 10));
        return new NodesDeprecationCheckRequest(newNodeIds);
    }

    @Override
    protected NodesDeprecationCheckRequest createTestInstance() {
        return new NodesDeprecationCheckRequest(randomArray(0, 10, String[]::new, () -> randomAlphaOfLengthBetween(5, 10)));
    }
}
