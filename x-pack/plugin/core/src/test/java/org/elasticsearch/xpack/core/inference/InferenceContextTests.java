/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.core.inference.InferenceContext.INFERENCE_ATTRIBUTION_HEADERS_ADDED;

public class InferenceContextTests extends AbstractWireSerializingTestCase<InferenceContext> {
    @Override
    protected Writeable.Reader<InferenceContext> instanceReader() {
        return InferenceContext::new;
    }

    @Override
    protected InferenceContext createTestInstance() {
        return createRandom();
    }

    public static InferenceContext createRandom() {
        return new InferenceContext(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    /**
     * Drops fields that a peer on {@code version} cannot serialize.
     */
    public static InferenceContext forTransportVersion(InferenceContext context, TransportVersion version) {
        if (version.supports(INFERENCE_ATTRIBUTION_HEADERS_ADDED) == false) {
            return new InferenceContext(context.productUseCase());
        }
        return context;
    }

    @Override
    protected InferenceContext mutateInstance(InferenceContext instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new InferenceContext(
                randomValueOtherThan(instance.productUseCase(), () -> randomAlphaOfLength(10)),
                instance.interactionId(),
                instance.productSolution(),
                instance.productFeature()
            );
            case 1 -> new InferenceContext(
                instance.productUseCase(),
                randomValueOtherThan(instance.interactionId(), () -> randomAlphaOfLength(10)),
                instance.productSolution(),
                instance.productFeature()
            );
            case 2 -> new InferenceContext(
                instance.productUseCase(),
                instance.interactionId(),
                randomValueOtherThan(instance.productSolution(), () -> randomAlphaOfLength(10)),
                instance.productFeature()
            );
            default -> new InferenceContext(
                instance.productUseCase(),
                instance.interactionId(),
                instance.productSolution(),
                randomValueOtherThan(instance.productFeature(), () -> randomAlphaOfLength(10))
            );
        };
    }

    public void testNewFieldsDroppedForOlderTransportVersion() throws IOException {
        var context = createRandom();
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(INFERENCE_ATTRIBUTION_HEADERS_ADDED);

        assertEquals(new InferenceContext(context.productUseCase()), copyInstance(context, oldVersion));
    }

    public void testRejectsNullFields() {
        expectThrows(NullPointerException.class, () -> new InferenceContext(null, "id", "solution", "feature"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", null, "solution", "feature"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "id", null, "feature"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "id", "solution", null));
    }
}
