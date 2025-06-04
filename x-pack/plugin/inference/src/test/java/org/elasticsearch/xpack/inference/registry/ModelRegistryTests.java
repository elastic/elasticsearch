/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.MinimalServiceSettingsTests;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModelRegistryTests extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry registry;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Before
    public void createComponents() {
        registry = node().injector().getInstance(ModelRegistry.class);
    }

    public void testGetUnparsedModelMap_ThrowsResourceNotFound_WhenNoHitsReturned() {
        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), is("Inference endpoint not found [1]"));
    }

    public void testGetModelWithSecrets() {
        assertStoreModel(
            registry,
            new TestModel(
                "1",
                TaskType.SPARSE_EMBEDDING,
                "foo",
                new TestModel.TestServiceSettings(null, null, null, null),
                new TestModel.TestTaskSettings(randomInt(3)),
                new TestModel.TestSecretSettings("secret")
            )
        );

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        var modelConfig = listener.actionGet(TIMEOUT);
        assertEquals("1", modelConfig.inferenceEntityId());
        assertEquals("foo", modelConfig.service());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelConfig.taskType());
        assertNotNull(modelConfig.settings().keySet());
        assertThat(modelConfig.secrets().keySet(), hasSize(1));
        assertThat(modelConfig.secrets().get("secret_settings"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        var secretSettings = (Map<String, Object>) modelConfig.secrets().get("secret_settings");
        assertThat(secretSettings.get("api_key"), equalTo("secret"));
    }

    public void testGetModelNoSecrets() {
        assertStoreModel(
            registry,
            new TestModel(
                "1",
                TaskType.SPARSE_EMBEDDING,
                "foo",
                new TestModel.TestServiceSettings(null, null, null, null),
                new TestModel.TestTaskSettings(randomInt(3)),
                new TestModel.TestSecretSettings(randomAlphaOfLength(4))
            )
        );

        var getListener = new PlainActionFuture<UnparsedModel>();
        registry.getModel("1", getListener);

        var modelConfig = getListener.actionGet(TIMEOUT);
        assertEquals("1", modelConfig.inferenceEntityId());
        assertEquals("foo", modelConfig.service());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelConfig.taskType());
        assertNotNull(modelConfig.settings().keySet());
        assertThat(modelConfig.secrets().keySet(), empty());
    }

    public void testStoreModel_ReturnsTrue_WhenNoFailuresOccur() {
        var model = TestModel.createRandomInstance();
        assertStoreModel(registry, model);
    }

    public void testStoreModel_ThrowsResourceAlreadyExistsException_WhenFailureIsAVersionConflict() {
        var model = TestModel.createRandomInstance();
        assertStoreModel(registry, model);

        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> assertStoreModel(registry, model)
        );
        assertThat(
            exception.getMessage(),
            is(format("Inference endpoint [%s] already exists", model.getConfigurations().getInferenceEntityId()))
        );
    }

    public void testRemoveDefaultConfigs_DoesNotCallClient_WhenPassedAnEmptySet() {
        var listener = new PlainActionFuture<Boolean>();
        registry.removeDefaultConfigs(Set.of(), listener);
        assertTrue(listener.actionGet(TIMEOUT));
    }

    public void testDeleteModels_Returns_ConflictException_WhenModelIsBeingAdded() {
        var model = TestModel.createRandomInstance();
        var newModel = TestModel.createRandomInstance();
        registry.updateModelTransaction(newModel, model, new PlainActionFuture<>());

        var listener = new PlainActionFuture<Boolean>();

        registry.deleteModels(Set.of(newModel.getInferenceEntityId()), listener);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            containsString("are currently being updated, please wait until after they are finished updating to delete.")
        );
        assertThat(exception.status(), is(RestStatus.CONFLICT));
    }

    public void testIdMatchedDefault() {
        var defaultConfigIds = new ArrayList<InferenceService.DefaultConfigId>();
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId("foo", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId("bar", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );

        var matched = ModelRegistry.idMatchedDefault("bar", defaultConfigIds);
        assertEquals(defaultConfigIds.get(1), matched.get());
        matched = ModelRegistry.idMatchedDefault("baz", defaultConfigIds);
        assertFalse(matched.isPresent());
    }

    public void testContainsDefaultConfigId() {
        registry.addDefaultIds(
            new InferenceService.DefaultConfigId("foo", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        registry.addDefaultIds(
            new InferenceService.DefaultConfigId("bar", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        assertTrue(registry.containsDefaultConfigId("foo"));
        assertFalse(registry.containsDefaultConfigId("baz"));
    }

    public void testTaskTypeMatchedDefaults() {
        var defaultConfigIds = new ArrayList<InferenceService.DefaultConfigId>();
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId("s1", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId("s2", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId(
                "d1",
                MinimalServiceSettings.textEmbedding("my_service", 384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                mock(InferenceService.class)
            )
        );
        defaultConfigIds.add(
            new InferenceService.DefaultConfigId("c1", MinimalServiceSettings.completion("my_service"), mock(InferenceService.class))
        );

        var matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.SPARSE_EMBEDDING, defaultConfigIds);
        assertThat(matched, contains(defaultConfigIds.get(0), defaultConfigIds.get(1)));
        matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.TEXT_EMBEDDING, defaultConfigIds);
        assertThat(matched, contains(defaultConfigIds.get(2)));
        matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.RERANK, defaultConfigIds);
        assertThat(matched, empty());
    }

    public void testDuplicateDefaultIds() {
        var id = "my-inference";
        var mockServiceA = mock(InferenceService.class);
        when(mockServiceA.name()).thenReturn("service-a");
        var mockServiceB = mock(InferenceService.class);
        when(mockServiceB.name()).thenReturn("service-b");

        registry.addDefaultIds(new InferenceService.DefaultConfigId(id, MinimalServiceSettingsTests.randomInstance(), mockServiceA));
        var ise = expectThrows(
            IllegalStateException.class,
            () -> registry.addDefaultIds(
                new InferenceService.DefaultConfigId(id, MinimalServiceSettingsTests.randomInstance(), mockServiceB)
            )
        );
        assertThat(
            ise.getMessage(),
            containsString(
                "Cannot add default endpoint to the inference endpoint registry with duplicate inference id [my-inference] declared by "
                    + "service [service-b]. The inference Id is already use by [service-a] service."
            )
        );
    }

    public static void assertStoreModel(ModelRegistry registry, Model model) {
        PlainActionFuture<Boolean> storeListener = new PlainActionFuture<>();
        registry.storeModel(model, storeListener, TimeValue.THIRTY_SECONDS);
        assertTrue(storeListener.actionGet(TimeValue.THIRTY_SECONDS));

        var settings = registry.getMinimalServiceSettings(model.getInferenceEntityId());
        assertNotNull(settings);
        assertThat(settings.taskType(), equalTo(model.getTaskType()));
        assertThat(settings.dimensions(), equalTo(model.getServiceSettings().dimensions()));
        assertThat(settings.elementType(), equalTo(model.getServiceSettings().elementType()));
        assertThat(settings.dimensions(), equalTo(model.getServiceSettings().dimensions()));
    }
}
