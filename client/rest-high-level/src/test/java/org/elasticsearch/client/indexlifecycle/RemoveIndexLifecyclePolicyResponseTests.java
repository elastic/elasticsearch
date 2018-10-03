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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class RemoveIndexLifecyclePolicyResponseTests extends ESTestCase {

    private void toXContent(RemoveIndexLifecyclePolicyResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(RemoveIndexLifecyclePolicyResponse.HAS_FAILURES_FIELD.getPreferredName(), response.hasFailures());
        builder.field(RemoveIndexLifecyclePolicyResponse.FAILED_INDEXES_FIELD.getPreferredName(), response.getFailedIndexes());
        builder.endObject();
    }

    private RemoveIndexLifecyclePolicyResponse createTestInstance() {
        List<String> failedIndexes = Arrays.asList(generateRandomStringArray(20, 20, false));
        return new RemoveIndexLifecyclePolicyResponse(failedIndexes);
    }

    public void testFromXContent() throws IOException {
        xContentTester(
                this::createParser,
                this::createTestInstance,
                this::toXContent,
                RemoveIndexLifecyclePolicyResponse::fromXContent)
                .supportsUnknownFields(true)
                .test();
    }

    public void testNullFailedIndices() {
        IllegalArgumentException exception =
                expectThrows(IllegalArgumentException.class, () -> new RemoveIndexLifecyclePolicyResponse(null));
        assertEquals("failed_indexes cannot be null", exception.getMessage());
    }

    public void testHasFailures() {
        RemoveIndexLifecyclePolicyResponse response = new RemoveIndexLifecyclePolicyResponse(new ArrayList<>());
        assertFalse(response.hasFailures());
        assertEquals(Collections.emptyList(), response.getFailedIndexes());

        int size = randomIntBetween(1, 10);
        List<String> failedIndexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            failedIndexes.add(randomAlphaOfLength(20));
        }
        response = new RemoveIndexLifecyclePolicyResponse(failedIndexes);
        assertTrue(response.hasFailures());
        assertEquals(failedIndexes, response.getFailedIndexes());
    }

    public void testEqualsAndHashCode() {
        RemoveIndexLifecyclePolicyResponse resp0 = new RemoveIndexLifecyclePolicyResponse(Collections.emptyList());
        RemoveIndexLifecyclePolicyResponse resp1 = new RemoveIndexLifecyclePolicyResponse(Collections.emptyList());
        RemoveIndexLifecyclePolicyResponse resp2 = new RemoveIndexLifecyclePolicyResponse(Collections.singletonList("baz"));
        RemoveIndexLifecyclePolicyResponse resp3 = new RemoveIndexLifecyclePolicyResponse(Collections.singletonList("baz"));
        RemoveIndexLifecyclePolicyResponse resp4 = new RemoveIndexLifecyclePolicyResponse(Collections.singletonList("rbh"));
        RemoveIndexLifecyclePolicyResponse resp5 = new RemoveIndexLifecyclePolicyResponse(Arrays.asList("baz", "rbh"));
        RemoveIndexLifecyclePolicyResponse resp6 = new RemoveIndexLifecyclePolicyResponse(Arrays.asList("baz", "rbh"));
        RemoveIndexLifecyclePolicyResponse resp7 = new RemoveIndexLifecyclePolicyResponse(Arrays.asList("baz", "foo"));

        assertEquals(resp0, resp1);
        assertEquals(resp0.hashCode(), resp1.hashCode());
        assertEquals(resp2, resp3);
        assertEquals(resp2.hashCode(), resp3.hashCode());
        assertEquals(resp5, resp6);
        assertEquals(resp5.hashCode(), resp6.hashCode());

        assertNotEquals(resp0, resp2);
        assertNotEquals(resp0, resp3);
        assertNotEquals(resp0, resp4);
        assertNotEquals(resp0, resp5);
        assertNotEquals(resp0, resp6);
        assertNotEquals(resp0, resp7);
        assertNotEquals(resp1, resp2);
        assertNotEquals(resp1, resp3);
        assertNotEquals(resp1, resp4);
        assertNotEquals(resp1, resp5);
        assertNotEquals(resp1, resp6);
        assertNotEquals(resp1, resp7);
        assertNotEquals(resp2, resp4);
        assertNotEquals(resp2, resp5);
        assertNotEquals(resp2, resp6);
        assertNotEquals(resp2, resp7);
        assertNotEquals(resp3, resp4);
        assertNotEquals(resp3, resp5);
        assertNotEquals(resp3, resp6);
        assertNotEquals(resp3, resp7);
        assertNotEquals(resp4, resp6);
        assertNotEquals(resp4, resp7);
        assertNotEquals(resp5, resp7);
        assertNotEquals(resp6, resp7);
    }
}
