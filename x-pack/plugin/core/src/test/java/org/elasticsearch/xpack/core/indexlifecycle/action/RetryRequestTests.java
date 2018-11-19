/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.action.RetryAction.Request;

import java.io.IOException;
import java.util.Arrays;

public class RetryRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request();
        if (randomBoolean()) {
            request.indices(generateRandomStringArray(20, 20, false));
        }
        if (randomBoolean()) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        switch (between(0, 1)) {
            case 0:
                indices = randomValueOtherThanMany(i -> Arrays.equals(i, instance.indices()),
                    () -> generateRandomStringArray(20, 10, false, true));
                break;
            case 1:
                indicesOptions = randomValueOtherThan(indicesOptions, () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        Request newRequest = new Request();
        newRequest.indices(indices);
        newRequest.indicesOptions(indicesOptions);
        return newRequest;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
