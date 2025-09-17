/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.AbstractTestInferenceService;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.registry.ModelRegistryTests.assertStoreModel;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class InferenceEndpointRegistryTests extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private InferenceEndpointRegistry inferenceEndpointRegistry;
    private ModelRegistry registry;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Before
    public void createComponents() {
        inferenceEndpointRegistry = node().injector().getInstance(InferenceEndpointRegistry.class);
        registry = node().injector().getInstance(ModelRegistry.class);
    }

    public void testGetThrowsResourceNotFoundWhenNoHitsReturned() {
        assertThat(
            getEndpointException("this is not found", ResourceNotFoundException.class).getMessage(),
            is("Inference endpoint not found [this is not found]")
        );
    }

    private <T extends Exception> Exception getEndpointException(String id, Class<T> expectedExceptionClass) {
        var listener = new PlainActionFuture<Model>();
        inferenceEndpointRegistry.getEndpoint(id, listener);
        return expectThrows(expectedExceptionClass, () -> listener.actionGet(TIMEOUT));
    }

    public void testGetModel() {
        var expectedEndpoint = storeWorkingEndpoint("1");
        var actualEndpoint = getEndpoint("1");
        assertThat(actualEndpoint, equalTo(expectedEndpoint));
        assertThat(getEndpoint("1"), sameInstance(actualEndpoint));
    }

    private Model storeWorkingEndpoint(String id) {
        var expectedEndpoint = new AbstractTestInferenceService.TestServiceModel(
            id,
            TaskType.SPARSE_EMBEDDING,
            "test_service",
            new TestSparseInferenceServiceExtension.TestServiceSettings("model", null, false),
            new AbstractTestInferenceService.TestTaskSettings(randomInt(3)),
            new AbstractTestInferenceService.TestSecretSettings("secret")
        );
        assertStoreModel(registry, expectedEndpoint);
        return expectedEndpoint;
    }

    private Model getEndpoint(String id) {
        var listener = new PlainActionFuture<Model>();
        inferenceEndpointRegistry.getEndpoint(id, listener);
        return listener.actionGet(TIMEOUT);
    }

    public void testGetModelWithUnknownService() {
        var id = "ahhhh";
        var expectedEndpoint = new AbstractTestInferenceService.TestServiceModel(
            id,
            TaskType.SPARSE_EMBEDDING,
            "hello",
            new TestSparseInferenceServiceExtension.TestServiceSettings("model", null, false),
            new AbstractTestInferenceService.TestTaskSettings(randomInt(3)),
            new AbstractTestInferenceService.TestSecretSettings("secret")
        );
        assertStoreModel(registry, expectedEndpoint);

        assertThat(
            getEndpointException(id, ResourceNotFoundException.class).getMessage(),
            equalTo("Unknown service [hello] for model [ahhhh]")
        );
    }
}
