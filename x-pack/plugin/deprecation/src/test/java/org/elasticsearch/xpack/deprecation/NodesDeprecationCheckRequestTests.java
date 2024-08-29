/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class NodesDeprecationCheckRequestTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            createTestInstance(),
            i -> new NodesDeprecationCheckRequest(i.nodesIds()),
            this::mutateInstance
        );
    }

    private NodesDeprecationCheckRequest mutateInstance(NodesDeprecationCheckRequest instance) {
        int newSize = randomValueOtherThan(instance.nodesIds().length, () -> randomIntBetween(0, 10));
        String[] newNodeIds = randomArray(newSize, newSize, String[]::new, () -> randomAlphaOfLengthBetween(5, 10));
        return new NodesDeprecationCheckRequest(newNodeIds);
    }

    private NodesDeprecationCheckRequest createTestInstance() {
        return new NodesDeprecationCheckRequest(randomArray(0, 10, String[]::new, () -> randomAlphaOfLengthBetween(5, 10)));
    }
}
