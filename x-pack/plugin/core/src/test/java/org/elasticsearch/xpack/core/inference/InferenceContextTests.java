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
        return new InferenceContext(randomAlphaOfLength(10));
    }

    @Override
    protected InferenceContext mutateInstance(InferenceContext instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
