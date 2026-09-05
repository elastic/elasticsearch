/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

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

    @Override
    protected InferenceContext mutateInstance(InferenceContext instance) throws IOException {
        var components = new String[] {
            instance.productUseCase(),
            instance.productSolution(),
            instance.productFeature(),
            instance.interactionId() };
        var i = randomIntBetween(0, components.length - 1);
        components[i] = randomValueOtherThan(components[i], () -> randomAlphaOfLength(10));

        return new InferenceContext(components[0], components[1], components[2], components[3]);
    }

    public void testRejectsNullComponents() {
        expectThrows(NullPointerException.class, () -> new InferenceContext(null, "solution", "feature", "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", null, "feature", "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "solution", null, "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "solution", "feature", null));
    }
}
