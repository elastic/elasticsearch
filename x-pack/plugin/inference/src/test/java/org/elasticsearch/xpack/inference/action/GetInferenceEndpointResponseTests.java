/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceEndpointAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

import java.io.IOException;
import java.util.ArrayList;

public class GetInferenceEndpointResponseTests extends AbstractWireSerializingTestCase<GetInferenceEndpointAction.Response> {
    @Override
    protected Writeable.Reader<GetInferenceEndpointAction.Response> instanceReader() {
        return GetInferenceEndpointAction.Response::new;
    }

    @Override
    protected GetInferenceEndpointAction.Response createTestInstance() {
        int numModels = randomIntBetween(1, 5);
        var modelConfigs = new ArrayList<ModelConfigurations>();
        for (int i = 0; i < numModels; i++) {
            modelConfigs.add(ModelConfigurationsTests.createRandomInstance());
        }
        return new GetInferenceEndpointAction.Response(modelConfigs);
    }

    @Override
    protected GetInferenceEndpointAction.Response mutateInstance(GetInferenceEndpointAction.Response instance) throws IOException {
        var modifiedConfigs = new ArrayList<>(instance.getModels());
        modifiedConfigs.add(ModelConfigurationsTests.createRandomInstance());
        return new GetInferenceEndpointAction.Response(modifiedConfigs);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }
}
