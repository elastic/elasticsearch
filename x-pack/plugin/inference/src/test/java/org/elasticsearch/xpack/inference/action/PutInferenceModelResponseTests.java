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
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

import java.util.ArrayList;
import java.util.List;

public class PutInferenceModelResponseTests extends AbstractWireSerializingTestCase<PutInferenceModelAction.Response> {

    @Override
    protected PutInferenceModelAction.Response createTestInstance() {
        return new PutInferenceModelAction.Response(ModelConfigurationsTests.createRandomInstance());
    }

    @Override
    protected PutInferenceModelAction.Response mutateInstance(PutInferenceModelAction.Response instance) {
        ModelConfigurations newModel = randomValueOtherThan(instance.getModel(), ModelConfigurationsTests::createRandomInstance);
        return new PutInferenceModelAction.Response(newModel);
    }

    @Override
    protected Writeable.Reader<PutInferenceModelAction.Response> instanceReader() {
        return PutInferenceModelAction.Response::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
    }
}
