/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;

public class NodesDeprecationCheckRequestTests
    extends AbstractStreamableTestCase<NodesDeprecationCheckRequest> {

    @Override
    protected NodesDeprecationCheckRequest createBlankInstance() {
        return new NodesDeprecationCheckRequest();
    }

    @Override
    protected NodesDeprecationCheckRequest mutateInstance(NodesDeprecationCheckRequest instance) throws IOException {
        int newSize = randomValueOtherThan(instance.nodesIds().length, () -> randomIntBetween(0,10));
        String[] newNodeIds = randomArray(newSize, newSize, String[]::new, () -> randomAlphaOfLengthBetween(5, 10));
        return new NodesDeprecationCheckRequest(newNodeIds);
    }

    @Override
    protected NodesDeprecationCheckRequest createTestInstance() {
        return new NodesDeprecationCheckRequest(randomArray(0, 10, String[]::new,
            ()-> randomAlphaOfLengthBetween(5,10)));
    }
}
