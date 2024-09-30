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
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

import java.io.IOException;
import java.util.ArrayList;

public class GetInferenceModelResponseTests extends AbstractWireSerializingTestCase<GetInferenceModelAction.Response> {
    @Override
    protected Writeable.Reader<GetInferenceModelAction.Response> instanceReader() {
        return GetInferenceModelAction.Response::new;
    }

    @Override
    protected GetInferenceModelAction.Response createTestInstance() {
        int numModels = randomIntBetween(1, 5);
        var modelConfigs = new ArrayList<ModelConfigurations>();
        for (int i = 0; i < numModels; i++) {
            modelConfigs.add(ModelConfigurationsTests.createRandomInstance());
        }
        return new GetInferenceModelAction.Response(modelConfigs);
    }

    @Override
    protected GetInferenceModelAction.Response mutateInstance(GetInferenceModelAction.Response instance) throws IOException {
        var modifiedConfigs = new ArrayList<>(instance.getEndpoints());
        modifiedConfigs.add(ModelConfigurationsTests.createRandomInstance());
        return new GetInferenceModelAction.Response(modifiedConfigs);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }
}
