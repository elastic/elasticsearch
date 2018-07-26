/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexPolicyResponse;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SetIndexPolicyResponseTests extends AbstractStreamableXContentTestCase<SetIndexPolicyResponse> {

    @Override
    protected SetIndexPolicyResponse createBlankInstance() {
        return new SetIndexPolicyResponse();
    }

    @Override
    protected SetIndexPolicyResponse createTestInstance() {
        List<String> failedIndexes = Arrays.asList(generateRandomStringArray(20, 20, false));
        return new SetIndexPolicyResponse(failedIndexes);
    }

    @Override
    protected SetIndexPolicyResponse mutateInstance(SetIndexPolicyResponse instance) throws IOException {
        List<String> failedIndices = randomValueOtherThan(instance.getFailedIndexes(),
                () -> Arrays.asList(generateRandomStringArray(20, 20, false)));
        return new SetIndexPolicyResponse(failedIndices);
    }

    @Override
    protected SetIndexPolicyResponse doParseInstance(XContentParser parser) throws IOException {
        return SetIndexPolicyResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testNullFailedIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new SetIndexPolicyResponse(null));
        assertEquals("failed_indexes cannot be null", exception.getMessage());
    }

    public void testHasFailures() {
        SetIndexPolicyResponse response = new SetIndexPolicyResponse(new ArrayList<>());
        assertFalse(response.hasFailures());
        assertEquals(Collections.emptyList(), response.getFailedIndexes());

        int size = randomIntBetween(1, 10);
        List<String> failedIndexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            failedIndexes.add(randomAlphaOfLength(20));
        }
        response = new SetIndexPolicyResponse(failedIndexes);
        assertTrue(response.hasFailures());
        assertEquals(failedIndexes, response.getFailedIndexes());
    }

}
