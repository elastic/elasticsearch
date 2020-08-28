/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
