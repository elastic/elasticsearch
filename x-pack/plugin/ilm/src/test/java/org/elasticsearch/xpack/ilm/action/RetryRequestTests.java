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

import java.util.Arrays;

public class RetryRequestTests extends AbstractWireSerializingTestCase<TransportRetryAction.Request> {

    @Override
    protected TransportRetryAction.Request createTestInstance() {
        final var request = new TransportRetryAction.Request(
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
        return request;
    }

    @Override
    protected Writeable.Reader<TransportRetryAction.Request> instanceReader() {
        return TransportRetryAction.Request::new;
    }

    @Override
    protected TransportRetryAction.Request mutateInstance(TransportRetryAction.Request instance) {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        switch (between(0, 1)) {
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
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        final var newRequest = new TransportRetryAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indices);
        newRequest.indicesOptions(indicesOptions);
        return newRequest;
    }
}
