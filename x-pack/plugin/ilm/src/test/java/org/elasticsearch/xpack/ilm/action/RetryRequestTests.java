/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */
package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.action.RetryActionRequest;

import java.util.Arrays;

public class RetryRequestTests extends AbstractWireSerializingTestCase<RetryActionRequest> {

    @Override
    protected RetryActionRequest createTestInstance() {
        final var request = new RetryActionRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomBoolean() ? Strings.EMPTY_ARRAY : generateRandomStringArray(20, 20, false)
        );
        if (randomBoolean()) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            request.indicesOptions(indicesOptions);
        }
        if (randomBoolean()) {
            request.requireError(randomBoolean());
        }
        return request;
    }

    @Override
    protected Writeable.Reader<RetryActionRequest> instanceReader() {
        return RetryActionRequest::new;
    }

    @Override
    protected RetryActionRequest mutateInstance(RetryActionRequest instance) {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        boolean requireError = instance.requireError();
        switch (between(0, 2)) {
            case 0 -> indices = randomValueOtherThanMany(
                i -> Arrays.equals(i, instance.indices()),
                () -> generateRandomStringArray(20, 10, false, true)
            );
            case 1 -> indicesOptions = randomValueOtherThan(
                indicesOptions,
                () -> IndicesOptions.fromOptions(
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean()
                )
            );
            case 2 -> requireError = requireError == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        final var newRequest = new RetryActionRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indices);
        newRequest.indicesOptions(indicesOptions);
        newRequest.requireError(requireError);
        return newRequest;
    }
}
