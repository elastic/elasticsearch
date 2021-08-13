/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
