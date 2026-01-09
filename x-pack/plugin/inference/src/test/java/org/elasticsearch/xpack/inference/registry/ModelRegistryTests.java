/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.MinimalServiceSettingsTests;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModelRegistryTests extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry registry;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    @Before
    public void createComponents() {
        registry = node().injector().getInstance(ModelRegistry.class);
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

    public void testContainsPreconfiguredInferenceEndpointId() {
        registry.addDefaultIds(
            new InferenceService.DefaultConfigId("foo", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        registry.addDefaultIds(
            new InferenceService.DefaultConfigId("bar", MinimalServiceSettings.sparseEmbedding("my_service"), mock(InferenceService.class))
        );
        assertTrue(registry.containsPreconfiguredInferenceEndpointId("foo"));
        assertFalse(registry.containsPreconfiguredInferenceEndpointId("baz"));
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

    public void testDeleteModels_Succeeds_WhenNoInferenceIdsAreProvided() {
        var model = TestModel.createRandomInstance();
        assertStoreModel(registry, model);

        var listener = new PlainActionFuture<Boolean>();
        registry.deleteModels(Set.of(), listener);
        assertTrue(listener.actionGet(TIMEOUT));
    }

    public static void assertStoreModel(ModelRegistry registry, Model model) {
        PlainActionFuture<Boolean> storeListener = new PlainActionFuture<>();
        registry.storeModel(model, storeListener, TimeValue.THIRTY_SECONDS);
        assertTrue(storeListener.actionGet(TimeValue.THIRTY_SECONDS));

        assertMinimalServiceSettings(registry, model);
    }

    public static void assertMinimalServiceSettings(ModelRegistry registry, Model model) {
        var settings = registry.getMinimalServiceSettings(model.getInferenceEntityId());
        assertNotNull(settings);
        assertThat(settings.taskType(), Matchers.equalTo(model.getTaskType()));
        assertThat(settings.dimensions(), Matchers.equalTo(model.getServiceSettings().dimensions()));
        assertThat(settings.elementType(), Matchers.equalTo(model.getServiceSettings().elementType()));
        assertThat(settings.dimensions(), Matchers.equalTo(model.getServiceSettings().dimensions()));
    }
}
