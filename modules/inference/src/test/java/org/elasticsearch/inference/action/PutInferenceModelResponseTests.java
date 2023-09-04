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
import org.elasticsearch.inference.ModelTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PutInferenceModelResponseTests extends AbstractWireSerializingTestCase<PutInferenceModelAction.Response> {

    @Override
    protected PutInferenceModelAction.Response createTestInstance() {
        return new PutInferenceModelAction.Response(ModelTests.createRandomInstance());
    }

    @Override
    protected PutInferenceModelAction.Response mutateInstance(PutInferenceModelAction.Response instance) {
        var mutatedModel = ModelTests.mutateTestInstance(instance.getModelConfig());
        return new PutInferenceModelAction.Response(mutatedModel);
    }

    @Override
    protected Writeable.Reader<PutInferenceModelAction.Response> instanceReader() {
        return PutInferenceModelAction.Response::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }
}
