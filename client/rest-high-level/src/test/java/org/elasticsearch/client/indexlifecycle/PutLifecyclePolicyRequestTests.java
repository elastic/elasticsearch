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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.client.indexlifecycle.LifecyclePolicyTests.createRandomPolicy;

public class PutLifecyclePolicyRequestTests extends ESTestCase {

    private PutLifecyclePolicyRequest createTestInstance() {
        return new PutLifecyclePolicyRequest(createRandomPolicy(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testValidation() {
        PutLifecyclePolicyRequest req = createTestInstance();
        assertFalse(req.validate().isPresent());
    }

    public void testNullPolicy() {
        try {
            new PutLifecyclePolicyRequest(null);
            fail("should not have been able to create a PutLifecyclePolicyRequest with null policy");
        } catch (IllegalArgumentException ex) {
            assertEquals("policy definition cannot be null", ex.getMessage());
        }
    }

    public void testNullPolicyName() {
        try {
            PutLifecyclePolicyRequest req = new PutLifecyclePolicyRequest(createRandomPolicy(randomFrom("", null)));
            fail("should not be able to create a PutLifecyclePolicyRequest with a null/empty policy name");
        } catch (IllegalArgumentException ex) {
            assertEquals("policy name must be present", ex.getMessage());
        }
    }

}
