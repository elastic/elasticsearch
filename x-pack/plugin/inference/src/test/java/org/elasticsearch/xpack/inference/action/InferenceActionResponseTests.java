/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultTests;

import java.io.IOException;

public class InferenceActionResponseTests extends AbstractWireSerializingTestCase<InferenceAction.Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<InferenceAction.Response> instanceReader() {
        return InferenceAction.Response::new;
    }

    @Override
    protected InferenceAction.Response createTestInstance() {
        return new InferenceAction.Response(SparseEmbeddingResultTests.createRandomResult());
    }

    @Override
    protected InferenceAction.Response mutateInstance(InferenceAction.Response instance) throws IOException {
        return null;
    }
}
