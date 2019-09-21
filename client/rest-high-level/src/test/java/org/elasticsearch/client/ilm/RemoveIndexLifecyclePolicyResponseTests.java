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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

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

    private RemoveIndexLifecyclePolicyResponse createInstance() {
        List<String> failedIndexes = Arrays.asList(generateRandomStringArray(20, 20, false));
        return new RemoveIndexLifecyclePolicyResponse(failedIndexes);
    }

    private RemoveIndexLifecyclePolicyResponse copyInstance(RemoveIndexLifecyclePolicyResponse req) {
        return new RemoveIndexLifecyclePolicyResponse(new ArrayList<>(req.getFailedIndexes()));
    }

    private RemoveIndexLifecyclePolicyResponse mutateInstance(RemoveIndexLifecyclePolicyResponse req) {
        return new RemoveIndexLifecyclePolicyResponse(randomValueOtherThan(req.getFailedIndexes(),
                () -> Arrays.asList(generateRandomStringArray(20, 20, false))));
    }

    public void testFromXContent() throws IOException {
        xContentTester(
                this::createParser,
                this::createInstance,
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
        for (int count = 0; count < 100; ++count) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createInstance(), this::copyInstance, this::mutateInstance);
        }
    }
}
