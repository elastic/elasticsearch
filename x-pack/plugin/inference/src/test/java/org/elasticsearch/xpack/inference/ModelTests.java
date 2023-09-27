/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ModelTests extends AbstractWireSerializingTestCase<Model> {

    public static Model createRandomInstance() {
        return new Model(ModelConfigurationsTests.createRandomInstance(), ModelSecretsTests.createRandomInstance());
    }

    public static Model mutateTestInstance(Model instance) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new Model(ModelConfigurationsTests.createRandomInstance(), instance.getSecrets());
            case 1 -> new Model(instance.getConfigurations(), ModelSecretsTests.createRandomInstance());
            default -> throw new IllegalStateException();
        };
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<Model> instanceReader() {
        return Model::new;
    }

    @Override
    protected Model createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected Model mutateInstance(Model instance) {
        return mutateTestInstance(instance);
    }
}
