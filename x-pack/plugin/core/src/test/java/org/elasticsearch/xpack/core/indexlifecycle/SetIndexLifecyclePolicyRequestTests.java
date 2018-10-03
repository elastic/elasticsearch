/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;
import java.util.Arrays;

public class SetIndexLifecyclePolicyRequestTests extends AbstractStreamableTestCase<SetIndexLifecyclePolicyRequest> {

    @Override
    protected SetIndexLifecyclePolicyRequest createTestInstance() {
        SetIndexLifecyclePolicyRequest request = new SetIndexLifecyclePolicyRequest(randomAlphaOfLength(20),
            generateRandomStringArray(20, 20, false));
        if (randomBoolean()) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean());
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    @Override
    protected SetIndexLifecyclePolicyRequest createBlankInstance() {
        return new SetIndexLifecyclePolicyRequest();
    }

    @Override
    protected SetIndexLifecyclePolicyRequest mutateInstance(SetIndexLifecyclePolicyRequest instance) throws IOException {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        String policy = instance.policy();
        switch (between(0, 2)) {
        case 0:
            indices = randomValueOtherThanMany(i -> Arrays.equals(i, instance.indices()),
                    () -> generateRandomStringArray(20, 20, false));
            break;
        case 1:
            indicesOptions = randomValueOtherThan(indicesOptions, () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            break;
        case 2:
            policy = randomValueOtherThan(policy, () -> randomAlphaOfLength(20));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        SetIndexLifecyclePolicyRequest newRequest = new SetIndexLifecyclePolicyRequest(policy, indices);
        newRequest.indicesOptions(indicesOptions);
        return newRequest;
    }

    public void testNullIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new SetIndexLifecyclePolicyRequest(randomAlphaOfLength(20), (String[]) null));
        assertEquals("indices cannot be null", exception.getMessage());
    }

    public void testNullPolicy() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new SetIndexLifecyclePolicyRequest(null, generateRandomStringArray(20, 20, false)));
        assertEquals("policy cannot be null", exception.getMessage());
    }

    public void testValidate() {
        SetIndexLifecyclePolicyRequest request = createTestInstance();
        assertNull(request.validate());
    }
}
