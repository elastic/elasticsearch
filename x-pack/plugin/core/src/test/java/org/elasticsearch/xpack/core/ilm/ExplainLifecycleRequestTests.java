/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;

public class ExplainLifecycleRequestTests extends AbstractWireSerializingTestCase<ExplainLifecycleRequest> {

    @Override
    protected ExplainLifecycleRequest createTestInstance() {
        ExplainLifecycleRequest request = new ExplainLifecycleRequest();
        if (randomBoolean()) {
            request.indices(generateRandomStringArray(20, 20, false, false));
        }
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
            request.onlyErrors(randomBoolean());
        }
        if (randomBoolean()) {
            request.onlyManaged(randomBoolean());
        }
        return request;
    }

    @Override
    protected ExplainLifecycleRequest mutateInstance(ExplainLifecycleRequest instance) throws IOException {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        boolean onlyErrors = instance.onlyErrors();
        boolean onlyManaged = instance.onlyManaged();
        switch (between(0, 3)) {
            case 0 -> indices = randomValueOtherThanMany(
                i -> Arrays.equals(i, instance.indices()),
                () -> generateRandomStringArray(20, 10, false, false)
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
            case 2 -> onlyErrors = onlyErrors == false;
            case 3 -> onlyManaged = onlyManaged == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        ExplainLifecycleRequest newRequest = new ExplainLifecycleRequest();
        newRequest.indices(indices);
        newRequest.indicesOptions(indicesOptions);
        newRequest.onlyErrors(onlyErrors);
        newRequest.onlyManaged(onlyManaged);
        return newRequest;
    }

    @Override
    protected Reader<ExplainLifecycleRequest> instanceReader() {
        return ExplainLifecycleRequest::new;
    }

}
