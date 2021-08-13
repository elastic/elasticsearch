/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.test.ESTestCase;

public class GetLifecyclePolicyRequestTests extends ESTestCase {

    private GetLifecyclePolicyRequest createTestInstance() {
        int numPolicies = randomIntBetween(0, 10);
        String[] policyNames = new String[numPolicies];
        for (int i = 0; i < numPolicies; i++) {
            policyNames[i] = "policy-" + randomAlphaOfLengthBetween(2, 5);
        }
        return new GetLifecyclePolicyRequest(policyNames);
    }

    public void testValidation() {
        GetLifecyclePolicyRequest request = createTestInstance();
        assertFalse(request.validate().isPresent());
    }

    public void testNullPolicyNameShouldFail() {
        expectThrows(IllegalArgumentException.class,
            () -> new GetLifecyclePolicyRequest(randomAlphaOfLengthBetween(2,20), null, randomAlphaOfLengthBetween(2,20)));
    }

}
