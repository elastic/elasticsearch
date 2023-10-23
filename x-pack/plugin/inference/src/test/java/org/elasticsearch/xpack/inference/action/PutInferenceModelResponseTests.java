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
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

public class PutInferenceModelResponseTests extends AbstractWireSerializingTestCase<PutInferenceModelAction.Response> {

    @Override
    protected PutInferenceModelAction.Response createTestInstance() {
        return new PutInferenceModelAction.Response(ModelConfigurationsTests.createRandomInstance());
    }

    @Override
    protected PutInferenceModelAction.Response mutateInstance(PutInferenceModelAction.Response instance) {
        var mutatedModel = ModelConfigurationsTests.mutateTestInstance(instance.getModel());
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
