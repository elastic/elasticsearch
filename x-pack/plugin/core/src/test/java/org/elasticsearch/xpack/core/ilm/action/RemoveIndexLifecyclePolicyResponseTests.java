/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RemoveIndexLifecyclePolicyResponseTests extends AbstractSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        List<String> failedIndexes = Arrays.asList(generateRandomStringArray(20, 20, false));
        return new Response(failedIndexes);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        List<String> failedIndices = randomValueOtherThan(instance.getFailedIndexes(),
                () -> Arrays.asList(generateRandomStringArray(20, 20, false)));
        return new Response(failedIndices);
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return Response.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testNullFailedIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new Response((List<String>) null));
        assertEquals("failed_indexes cannot be null", exception.getMessage());
    }

    public void testHasFailures() {
        Response response = new Response(new ArrayList<>());
        assertFalse(response.hasFailures());
        assertEquals(Collections.emptyList(), response.getFailedIndexes());

        int size = randomIntBetween(1, 10);
        List<String> failedIndexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            failedIndexes.add(randomAlphaOfLength(20));
        }
        response = new Response(failedIndexes);
        assertTrue(response.hasFailures());
        assertEquals(failedIndexes, response.getFailedIndexes());
    }

}
