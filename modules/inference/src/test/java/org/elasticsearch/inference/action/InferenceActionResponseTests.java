/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.inference.results.SparseEmbeddingResultTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

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
