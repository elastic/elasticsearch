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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

public class RemoveIndexLifecyclePolicyRequestTests extends ESTestCase {

    public void testNullIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new RemoveIndexLifecyclePolicyRequest(null));
        assertEquals("indices cannot be null", exception.getMessage());
    }

    public void testNullIndicesOptions() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new RemoveIndexLifecyclePolicyRequest(Collections.emptyList(), null));
        assertEquals("indices options cannot be null", exception.getMessage());
    }

    public void testValidate() {
        RemoveIndexLifecyclePolicyRequest request = new RemoveIndexLifecyclePolicyRequest(Collections.emptyList());
        assertFalse(request.validate().isPresent());
    }

    public void testEqualsAndHashCode() {
        RemoveIndexLifecyclePolicyRequest req0 = new RemoveIndexLifecyclePolicyRequest(Collections.singletonList("rbh"));
        RemoveIndexLifecyclePolicyRequest req1 = new RemoveIndexLifecyclePolicyRequest(Collections.singletonList("rbh"));
        RemoveIndexLifecyclePolicyRequest req2 = new RemoveIndexLifecyclePolicyRequest(Collections.singletonList("baz"));
        RemoveIndexLifecyclePolicyRequest req3 =
                new RemoveIndexLifecyclePolicyRequest(Collections.singletonList("rbh"), IndicesOptions.lenientExpandOpen());
        RemoveIndexLifecyclePolicyRequest req4 =
                new RemoveIndexLifecyclePolicyRequest(Collections.singletonList("rbh"), IndicesOptions.lenientExpandOpen());
        RemoveIndexLifecyclePolicyRequest req5 =
                new RemoveIndexLifecyclePolicyRequest(Arrays.asList("rbh", "baz"), IndicesOptions.lenientExpandOpen());
        RemoveIndexLifecyclePolicyRequest req6 =
                new RemoveIndexLifecyclePolicyRequest(Arrays.asList("rbh", "baz"), IndicesOptions.lenientExpandOpen());

        assertEquals(req0, req1);
        assertEquals(req0.hashCode(), req1.hashCode());
        assertEquals(req3, req4);
        assertEquals(req3.hashCode(), req4.hashCode());
        assertEquals(req5, req6);
        assertEquals(req5.hashCode(), req6.hashCode());

        assertNotEquals(req0, req2);
        assertNotEquals(req0, req3);
        assertNotEquals(req0, req4);
        assertNotEquals(req0, req5);
        assertNotEquals(req0, req6);
        assertNotEquals(req1, req2);
        assertNotEquals(req1, req3);
        assertNotEquals(req1, req4);
        assertNotEquals(req1, req5);
        assertNotEquals(req1, req6);
        assertNotEquals(req3, req5);
        assertNotEquals(req3, req6);
        assertNotEquals(req4, req5);
        assertNotEquals(req4, req6);
    }
}
