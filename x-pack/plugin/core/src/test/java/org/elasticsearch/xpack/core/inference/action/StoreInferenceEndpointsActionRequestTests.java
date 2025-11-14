/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.ModelTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;

public class StoreInferenceEndpointsActionRequestTests extends AbstractBWCWireSerializationTestCase<StoreInferenceEndpointsAction.Request> {

    @Override
    protected StoreInferenceEndpointsAction.Request mutateInstanceForVersion(
        StoreInferenceEndpointsAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<StoreInferenceEndpointsAction.Request> instanceReader() {
        return StoreInferenceEndpointsAction.Request::new;
    }

    @Override
    protected StoreInferenceEndpointsAction.Request createTestInstance() {
        return new StoreInferenceEndpointsAction.Request(randomList(5, ModelTests::randomModel), randomTimeValue());
    }

    @Override
    protected StoreInferenceEndpointsAction.Request mutateInstance(StoreInferenceEndpointsAction.Request instance) throws IOException {
        var newModels = new ArrayList<>(instance.getModels());
        newModels.add(ModelTests.randomModel());
        return new StoreInferenceEndpointsAction.Request(newModels, instance.masterNodeTimeout());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        var namedWriteables = new ArrayList<NamedWriteableRegistry.Entry>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, EmptySecretSettings.NAME, EmptySecretSettings::new));
        namedWriteables.addAll(ModelTests.getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
    }
}
