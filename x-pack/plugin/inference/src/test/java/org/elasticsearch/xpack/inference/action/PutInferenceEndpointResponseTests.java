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
import org.elasticsearch.xpack.core.inference.action.PutInferenceEndpointAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

public class PutInferenceEndpointResponseTests extends AbstractWireSerializingTestCase<PutInferenceEndpointAction.Response> {

    @Override
    protected PutInferenceEndpointAction.Response createTestInstance() {
        return new PutInferenceEndpointAction.Response(ModelConfigurationsTests.createRandomInstance());
    }

    @Override
    protected PutInferenceEndpointAction.Response mutateInstance(PutInferenceEndpointAction.Response instance) {
        var mutatedModel = ModelConfigurationsTests.mutateTestInstance(instance.getModel());
        return new PutInferenceEndpointAction.Response(mutatedModel);
    }

    @Override
    protected Writeable.Reader<PutInferenceEndpointAction.Response> instanceReader() {
        return PutInferenceEndpointAction.Response::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }
}
