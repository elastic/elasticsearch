/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class ModelStoreResponseTests extends AbstractBWCWireSerializationTestCase<ModelStoreResponse> {

    public static ModelStoreResponse randomModelStoreResponse() {
        return new ModelStoreResponse(
            randomAlphaOfLength(10),
            randomFrom(RestStatus.values()),
            randomBoolean() ? null : new IllegalStateException("Test exception")
        );
    }

    public void testFailed() {
        {
            var successResponse = new ModelStoreResponse("model_1", RestStatus.OK, null);
            assertFalse(successResponse.failed());
        }
        {
            var failedResponse = new ModelStoreResponse(
                "model_2",
                RestStatus.INTERNAL_SERVER_ERROR,
                new IllegalStateException("Test failure")
            );
            assertTrue(failedResponse.failed());
        }
        {
            var failedResponse = new ModelStoreResponse("model_2", RestStatus.OK, new IllegalStateException("Test failure"));
            assertTrue(failedResponse.failed());
        }
    }

    @Override
    protected ModelStoreResponse mutateInstanceForVersion(ModelStoreResponse instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<ModelStoreResponse> instanceReader() {
        return ModelStoreResponse::new;
    }

    @Override
    protected ModelStoreResponse createTestInstance() {
        return randomModelStoreResponse();
    }

    @Override
    protected ModelStoreResponse mutateInstance(ModelStoreResponse instance) throws IOException {
        int choice = randomIntBetween(0, 2);
        return switch (choice) {
            case 0 -> {
                String newInferenceId = instance.inferenceId() + "_mutated";
                yield new ModelStoreResponse(newInferenceId, instance.status(), instance.failureCause());
            }
            case 1 -> new ModelStoreResponse(
                instance.inferenceId(),
                randomValueOtherThan(instance.status(), () -> randomFrom(RestStatus.values())),
                instance.failureCause()
            );
            case 2 -> {
                Exception newFailureCause = instance.failureCause() == null ? new IllegalStateException("Mutated exception") : null;
                yield new ModelStoreResponse(instance.inferenceId(), instance.status(), newFailureCause);
            }
            default -> throw new IllegalStateException("Unexpected value: " + choice);
        };
    }
}
