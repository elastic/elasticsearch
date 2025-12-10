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
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpdateInferenceModelActionResponseTests extends AbstractWireSerializingTestCase<UpdateInferenceModelAction.Response> {
    @Override
    protected Writeable.Reader<UpdateInferenceModelAction.Response> instanceReader() {
        return UpdateInferenceModelAction.Response::new;
    }

    @Override
    protected UpdateInferenceModelAction.Response createTestInstance() {
        return new UpdateInferenceModelAction.Response(ModelConfigurationsTests.createRandomInstance());
    }

    @Override
    protected UpdateInferenceModelAction.Response mutateInstance(UpdateInferenceModelAction.Response instance) throws IOException {
        ModelConfigurations newModel = randomValueOtherThan(instance.getModel(), ModelConfigurationsTests::createRandomInstance);
        return new UpdateInferenceModelAction.Response(newModel);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
    }
}
