/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.client.ilm.LifecyclePolicyTests.createRandomPolicy;

public class PutLifecyclePolicyRequestTests extends ESTestCase {

    private PutLifecyclePolicyRequest createTestInstance() {
        return new PutLifecyclePolicyRequest(createRandomPolicy(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testValidation() {
        PutLifecyclePolicyRequest req = createTestInstance();
        assertFalse(req.validate().isPresent());
    }

    public void testNullPolicy() {
        expectThrows(IllegalArgumentException.class, () -> new PutLifecyclePolicyRequest(null));
    }

    public void testNullPolicyName() {
        expectThrows(IllegalArgumentException.class, () -> new PutLifecyclePolicyRequest(createRandomPolicy(randomFrom("", null))));
    }

}
