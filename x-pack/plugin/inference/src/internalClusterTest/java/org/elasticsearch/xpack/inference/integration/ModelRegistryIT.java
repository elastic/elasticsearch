/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.MinimalServiceSettingsTests;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ModelRegistryTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserMlNodeTaskSettingsTests;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ModelRegistryIT extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry modelRegistry;

    @Before
    public void createComponents() {
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        modelRegistry.clearDefaultIds();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testStoreModel() throws Exception {
        String inferenceEntityId = "test-store-model";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        ModelRegistryTests.assertStoreModel(modelRegistry, model);
    }

    public void testStoreModelWithUnknownFields() throws Exception {
        String inferenceEntityId = "test-store-model-unknown-field";
        Model model = buildModelWithUnknownField(inferenceEntityId);
        ElasticsearchStatusException statusException = expectThrows(
            ElasticsearchStatusException.class,
            () -> ModelRegistryTests.assertStoreModel(modelRegistry, model)
        );
        assertThat(
            statusException.getRootCause().getMessage(),
            containsString("mapping set to strict, dynamic introduction of [unknown_field] within [_doc] is not allowed")
        );
        assertThat(statusException.getMessage(), containsString("Failed to store inference endpoint [" + inferenceEntityId + "]"));
    }

    public void testGetModel() throws Exception {
        String inferenceEntityId = "test-get-model";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        ModelRegistryTests.assertStoreModel(modelRegistry, model);

        // now get the model
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(modelHolder.get(), not(nullValue()));

        assertEquals(model.getConfigurations().getService(), modelHolder.get().service());

        var elserService = new ElasticsearchInternalService(
            new InferenceServiceExtension.InferenceServiceFactoryContext(
                mock(Client.class),
                mock(ThreadPool.class),
                mock(ClusterService.class),
                Settings.EMPTY
            )
        );
        ElasticsearchInternalModel roundTripModel = (ElasticsearchInternalModel) elserService.parsePersistedConfigWithSecrets(
            modelHolder.get().inferenceEntityId(),
            modelHolder.get().taskType(),
            modelHolder.get().settings(),
            modelHolder.get().secrets()
        );
        assertEquals(model, roundTripModel);
    }

    public void testStoreModelFailsWhenModelExists() throws Exception {
        String inferenceEntityId = "test-put-trained-model-config-exists";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        ModelRegistryTests.assertStoreModel(modelRegistry, model);

        // a model with the same id exists
        var exc = expectThrows(Exception.class, () -> ModelRegistryTests.assertStoreModel(modelRegistry, model));
        assertThat(exc.getMessage(), containsString("Inference endpoint [test-put-trained-model-config-exists] already exists"));
    }

    public void testDeleteModel() throws Exception {
        // put models
        for (var id : new String[] { "model1", "model2", "model3" }) {
            Model model = buildElserModelConfig(id, TaskType.SPARSE_EMBEDDING);
            ModelRegistryTests.assertStoreModel(modelRegistry, model);
        }

        AtomicReference<Boolean> deleteResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.deleteModel("model1", listener), deleteResponseHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertTrue(deleteResponseHolder.get());

        // get should fail
        deleteResponseHolder.set(false);
        AtomicReference<UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets("model1", listener), modelHolder, exceptionHolder);

        assertThat(exceptionHolder.get(), not(nullValue()));
        assertFalse(deleteResponseHolder.get());
        assertThat(exceptionHolder.get().getMessage(), containsString("Inference endpoint not found [model1]"));
    }

    public void testNonExistentDeleteModel_DoesNotThrowAnException() {
        var listener = new PlainActionFuture<Boolean>();

        modelRegistry.deleteModel("non-existent-model", listener);
        assertTrue(listener.actionGet(TIMEOUT));
    }

    public void testRemoveDefaultConfigs_DoesNotThrowAnException_WhenSearchingForNonExistentInferenceEndpointIds() {
        var listener = new PlainActionFuture<Boolean>();

        modelRegistry.deleteModels(Set.of("non-existent-model", "abc"), listener);
        assertTrue(listener.actionGet(TIMEOUT));
    }

    public void testRemoveDefaultConfigs_RemovesModelsFromPersistentStorage_AndInMemoryCache() {
        var service = mock(InferenceService.class);

        var defaultConfigs = new ArrayList<Model>();
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();
        for (var id : new String[] { "model1", "model2", "model3" }) {
            var modelSettings = MinimalServiceSettingsTests.randomInstance();
            defaultConfigs.add(createModel(id, modelSettings.taskType(), "name"));
            defaultIds.add(new InferenceService.DefaultConfigId(id, modelSettings, service));
        }

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(defaultConfigs);
            return Void.TYPE;
        }).when(service).defaultConfigs(any());

        defaultIds.forEach(modelRegistry::addDefaultIds);

        var getModelsListener = new PlainActionFuture<List<UnparsedModel>>();
        modelRegistry.getAllModels(true, getModelsListener);
        var unparsedModels = getModelsListener.actionGet(TIMEOUT);
        assertThat(unparsedModels.size(), is(3));

        var removeModelsListener = new PlainActionFuture<Boolean>();

        modelRegistry.removeDefaultConfigs(Set.of("model1", "model2", "model3"), removeModelsListener);
        assertTrue(removeModelsListener.actionGet(TIMEOUT));

        var getModelsAfterDeleteListener = new PlainActionFuture<List<UnparsedModel>>();
        // the models should have been removed from the in memory cache, if not they they will be persisted again by this call
        modelRegistry.getAllModels(true, getModelsAfterDeleteListener);
        var unparsedModelsAfterDelete = getModelsAfterDeleteListener.actionGet(TIMEOUT);
        assertThat(unparsedModelsAfterDelete.size(), is(0));
    }

    public void testGetModelsByTaskType() throws InterruptedException {
        var service = "foo";
        var sparseAndTextEmbeddingModels = new ArrayList<Model>();
        sparseAndTextEmbeddingModels.add(createModel(randomAlphaOfLength(5), TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel(randomAlphaOfLength(5), TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel(randomAlphaOfLength(5), TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel(randomAlphaOfLength(5), TaskType.TEXT_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel(randomAlphaOfLength(5), TaskType.TEXT_EMBEDDING, service));

        for (var model : sparseAndTextEmbeddingModels) {
            ModelRegistryTests.assertStoreModel(modelRegistry, model);
        }

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.SPARSE_EMBEDDING, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get(), hasSize(3));
        var sparseIds = sparseAndTextEmbeddingModels.stream()
            .filter(m -> m.getConfigurations().getTaskType() == TaskType.SPARSE_EMBEDDING)
            .map(Model::getInferenceEntityId)
            .collect(Collectors.toSet());
        modelHolder.get().forEach(m -> {
            assertTrue(sparseIds.contains(m.inferenceEntityId()));
            assertThat(m.secrets().keySet(), empty());
        });

        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.TEXT_EMBEDDING, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get(), hasSize(2));
        var denseIds = sparseAndTextEmbeddingModels.stream()
            .filter(m -> m.getConfigurations().getTaskType() == TaskType.TEXT_EMBEDDING)
            .map(Model::getInferenceEntityId)
            .collect(Collectors.toSet());
        modelHolder.get().forEach(m -> {
            assertTrue(denseIds.contains(m.inferenceEntityId()));
            assertThat(m.secrets().keySet(), empty());
        });
    }

    public void testGetAllModels() throws InterruptedException {
        var service = "foo";
        var createdModels = new ArrayList<Model>();
        int modelCount = randomIntBetween(30, 100);

        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        for (int i = 0; i < modelCount; i++) {
            var model = createModel(randomAlphaOfLength(5), randomFrom(TaskType.values()), service);
            createdModels.add(model);
            ModelRegistryTests.assertStoreModel(modelRegistry, model);
        }

        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getAllModels(randomBoolean(), listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(modelCount));
        var getAllModels = modelHolder.get();

        // sort in the same order as the returned models
        createdModels.sort(Comparator.comparing(Model::getInferenceEntityId));
        for (int i = 0; i < modelCount; i++) {
            assertEquals(createdModels.get(i).getInferenceEntityId(), getAllModels.get(i).inferenceEntityId());
            assertEquals(createdModels.get(i).getTaskType(), getAllModels.get(i).taskType());
            assertEquals(createdModels.get(i).getConfigurations().getService(), getAllModels.get(i).service());
            assertThat(getAllModels.get(i).secrets().keySet(), empty());
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithSecrets() throws InterruptedException {
        var service = "foo";
        var inferenceEntityId = "model-with-secrets";
        var secret = "abc";

        var modelWithSecrets = createModelWithSecrets(inferenceEntityId, randomFrom(TaskType.values()), service, secret);
        ModelRegistryTests.assertStoreModel(modelRegistry, modelWithSecrets);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get().secrets().keySet(), hasSize(1));
        var secretSettings = (Map<String, Object>) modelHolder.get().secrets().get("secret_settings");
        assertThat(secretSettings.get("secret"), equalTo(secret));
        assertReturnModelIsModifiable(modelHolder.get());

        // get model without secrets
        blockingCall(listener -> modelRegistry.getModel(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get().secrets().keySet(), empty());
        assertReturnModelIsModifiable(modelHolder.get());
    }

    public void testGetAllModels_WithDefaults() throws Exception {
        var serviceName = "foo";
        int configuredModelCount = 10;
        int defaultModelCount = 2;
        int totalModelCount = 12;

        var service = mock(InferenceService.class);

        var defaultConfigs = new ArrayList<Model>();
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();
        for (int i = 0; i < defaultModelCount; i++) {
            var id = "default-" + i;
            var modelSettings = MinimalServiceSettingsTests.randomInstance();
            defaultConfigs.add(createModel(id, modelSettings.taskType(), serviceName));
            defaultIds.add(new InferenceService.DefaultConfigId(id, modelSettings, service));
        }

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(defaultConfigs);
            return Void.TYPE;
        }).when(service).defaultConfigs(any());

        defaultIds.forEach(modelRegistry::addDefaultIds);

        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        var createdModels = new HashMap<String, Model>();
        for (int i = 0; i < configuredModelCount; i++) {
            var id = randomAlphaOfLength(5) + i;
            var model = createModel(id, randomFrom(TaskType.values()), serviceName);
            createdModels.put(id, model);
            ModelRegistryTests.assertStoreModel(modelRegistry, model);
        }

        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getAllModels(randomBoolean(), listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(totalModelCount));
        var getAllModels = modelHolder.get();
        assertReturnModelIsModifiable(modelHolder.get().get(0));

        // same result but configs should have been persisted this time
        blockingCall(listener -> modelRegistry.getAllModels(randomBoolean(), listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(totalModelCount));

        // sort in the same order as the returned models
        var ids = new ArrayList<>(defaultIds.stream().map(InferenceService.DefaultConfigId::inferenceId).toList());
        ids.addAll(createdModels.keySet().stream().toList());
        ids.sort(String::compareTo);
        var configsById = defaultConfigs.stream().collect(Collectors.toMap(Model::getInferenceEntityId, Function.identity()));
        for (int i = 0; i < totalModelCount; i++) {
            var id = ids.get(i);
            assertEquals(id, getAllModels.get(i).inferenceEntityId());
            if (id.startsWith("default")) {
                assertEquals(configsById.get(id).getTaskType(), getAllModels.get(i).taskType());
                assertEquals(configsById.get(id).getConfigurations().getService(), getAllModels.get(i).service());
            } else {
                assertEquals(createdModels.get(id).getTaskType(), getAllModels.get(i).taskType());
                assertEquals(createdModels.get(id).getConfigurations().getService(), getAllModels.get(i).service());
            }
        }
    }

    public void testGetAllModels_OnlyDefaults() throws Exception {
        int defaultModelCount = 2;
        var serviceName = "foo";
        var service = mock(InferenceService.class);

        var defaultConfigs = new ArrayList<Model>();
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();
        for (int i = 0; i < defaultModelCount; i++) {
            var id = "default-" + i;
            var modelSettings = MinimalServiceSettingsTests.randomInstance();
            defaultConfigs.add(createModel(id, modelSettings.taskType(), serviceName));
            defaultIds.add(new InferenceService.DefaultConfigId(id, modelSettings, service));
        }

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(defaultConfigs);
            return Void.TYPE;
        }).when(service).defaultConfigs(any());
        defaultIds.forEach(modelRegistry::addDefaultIds);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getAllModels(randomBoolean(), listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(2));
        var getAllModels = modelHolder.get();
        assertReturnModelIsModifiable(modelHolder.get().get(0));

        // sort in the same order as the returned models
        var configsById = defaultConfigs.stream().collect(Collectors.toMap(Model::getInferenceEntityId, Function.identity()));
        var ids = new ArrayList<>(configsById.keySet().stream().toList());
        ids.sort(String::compareTo);
        for (int i = 0; i < defaultModelCount; i++) {
            var id = ids.get(i);
            assertEquals(id, getAllModels.get(i).inferenceEntityId());
            assertEquals(configsById.get(id).getTaskType(), getAllModels.get(i).taskType());
            assertEquals(configsById.get(id).getConfigurations().getService(), getAllModels.get(i).service());
        }
    }

    public void testGetAllModels_withDoNotPersist() throws Exception {
        int defaultModelCount = 2;
        var serviceName = "foo";
        var service = mock(InferenceService.class);

        var defaultConfigs = new ArrayList<Model>();
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();
        for (int i = 0; i < defaultModelCount; i++) {
            var id = "default-" + i;
            var modelSettings = MinimalServiceSettingsTests.randomInstance();
            defaultConfigs.add(createModel(id, modelSettings.taskType(), serviceName));
            defaultIds.add(new InferenceService.DefaultConfigId(id, modelSettings, service));
        }

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(defaultConfigs);
            return Void.TYPE;
        }).when(service).defaultConfigs(any());

        defaultIds.forEach(modelRegistry::addDefaultIds);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getAllModels(false, listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(2));

        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(".inference").get()
        );

        // this time check the index is created
        blockingCall(listener -> modelRegistry.getAllModels(true, listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(2));
        assertInferenceIndexExists();
    }

    public void testGet_WithDefaults() throws InterruptedException {
        var serviceName = "foo";
        var service = mock(InferenceService.class);

        var defaultConfigs = new ArrayList<Model>();
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();

        defaultConfigs.add(createModel("default-sparse", TaskType.SPARSE_EMBEDDING, serviceName));
        defaultConfigs.add(createModel("default-text", TaskType.TEXT_EMBEDDING, serviceName));
        defaultIds.add(
            new InferenceService.DefaultConfigId("default-sparse", MinimalServiceSettings.sparseEmbedding(serviceName), service)
        );
        defaultIds.add(
            new InferenceService.DefaultConfigId(
                "default-text",
                MinimalServiceSettings.textEmbedding(serviceName, 384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                service
            )
        );

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(defaultConfigs);
            return Void.TYPE;
        }).when(service).defaultConfigs(any());
        defaultIds.forEach(modelRegistry::addDefaultIds);

        var configured1 = createModel(randomAlphaOfLength(5) + 1, randomFrom(TaskType.values()), serviceName);
        var configured2 = createModel(randomAlphaOfLength(5) + 1, randomFrom(TaskType.values()), serviceName);
        ModelRegistryTests.assertStoreModel(modelRegistry, configured1);
        ModelRegistryTests.assertStoreModel(modelRegistry, configured2);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModel("default-sparse", listener), modelHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("default-sparse", modelHolder.get().inferenceEntityId());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelHolder.get().taskType());
        assertReturnModelIsModifiable(modelHolder.get());

        blockingCall(listener -> modelRegistry.getModel("default-text", listener), modelHolder, exceptionHolder);
        assertEquals("default-text", modelHolder.get().inferenceEntityId());
        assertEquals(TaskType.TEXT_EMBEDDING, modelHolder.get().taskType());

        blockingCall(listener -> modelRegistry.getModel(configured1.getInferenceEntityId(), listener), modelHolder, exceptionHolder);
        assertEquals(configured1.getInferenceEntityId(), modelHolder.get().inferenceEntityId());
        assertEquals(configured1.getTaskType(), modelHolder.get().taskType());
    }

    public void testGetByTaskType_WithDefaults() throws Exception {
        var serviceName = "foo";

        var defaultSparse = createModel("default-sparse", TaskType.SPARSE_EMBEDDING, serviceName);
        var defaultText = createModel("default-text", TaskType.TEXT_EMBEDDING, serviceName);
        var defaultChat = createModel("default-chat", TaskType.COMPLETION, serviceName);

        var service = mock(InferenceService.class);
        var defaultIds = new ArrayList<InferenceService.DefaultConfigId>();
        defaultIds.add(
            new InferenceService.DefaultConfigId("default-sparse", MinimalServiceSettings.sparseEmbedding(serviceName), service)
        );
        defaultIds.add(
            new InferenceService.DefaultConfigId(
                "default-text",
                MinimalServiceSettings.textEmbedding(serviceName, 384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                service
            )
        );
        defaultIds.add(new InferenceService.DefaultConfigId("default-chat", MinimalServiceSettings.completion(serviceName), service));

        doAnswer(invocation -> {
            ActionListener<List<Model>> listener = invocation.getArgument(0);
            listener.onResponse(List.of(defaultSparse, defaultChat, defaultText));
            return Void.TYPE;
        }).when(service).defaultConfigs(any());
        defaultIds.forEach(modelRegistry::addDefaultIds);

        var configuredSparse = createModel("configured-sparse", TaskType.SPARSE_EMBEDDING, serviceName);
        var configuredText = createModel("configured-text", TaskType.TEXT_EMBEDDING, serviceName);
        var configuredRerank = createModel("configured-rerank", TaskType.RERANK, serviceName);
        ModelRegistryTests.assertStoreModel(modelRegistry, configuredSparse);
        ModelRegistryTests.assertStoreModel(modelRegistry, configuredText);
        ModelRegistryTests.assertStoreModel(modelRegistry, configuredRerank);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<List<UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.SPARSE_EMBEDDING, listener), modelHolder, exceptionHolder);
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        assertNull(exceptionHolder.get());
        assertThat(modelHolder.get(), hasSize(2));
        assertEquals("configured-sparse", modelHolder.get().get(0).inferenceEntityId());
        assertEquals("default-sparse", modelHolder.get().get(1).inferenceEntityId());

        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.TEXT_EMBEDDING, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get(), hasSize(2));
        assertEquals("configured-text", modelHolder.get().get(0).inferenceEntityId());
        assertEquals("default-text", modelHolder.get().get(1).inferenceEntityId());
        assertReturnModelIsModifiable(modelHolder.get().get(0));

        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.RERANK, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get(), hasSize(1));
        assertEquals("configured-rerank", modelHolder.get().get(0).inferenceEntityId());

        blockingCall(listener -> modelRegistry.getModelsByTaskType(TaskType.COMPLETION, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get(), hasSize(1));
        assertEquals("default-chat", modelHolder.get().get(0).inferenceEntityId());
        assertReturnModelIsModifiable(modelHolder.get().get(0));
    }

    private void assertInferenceIndexExists() {
        var indexResponse = client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(".inference").get();
        assertNotNull(indexResponse.getSettings());
        assertNotNull(indexResponse.getMappings());
    }

    @SuppressWarnings("unchecked")
    private void assertReturnModelIsModifiable(UnparsedModel unparsedModel) {
        var settings = unparsedModel.settings();
        if (settings != null) {
            var serviceSettings = (Map<String, Object>) settings.get("service_settings");
            if (serviceSettings != null && serviceSettings.size() > 0) {
                var itr = serviceSettings.entrySet().iterator();
                itr.next();
                itr.remove();
            }

            var taskSettings = (Map<String, Object>) settings.get("task_settings");
            if (taskSettings != null && taskSettings.size() > 0) {
                var itr = taskSettings.entrySet().iterator();
                itr.next();
                itr.remove();
            }

            if (unparsedModel.secrets() != null && unparsedModel.secrets().size() > 0) {
                var itr = unparsedModel.secrets().entrySet().iterator();
                itr.next();
                itr.remove();
            }
        }
    }

    private Model buildElserModelConfig(String inferenceEntityId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalModel(
                inferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                ElserInternalServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom(),
                randomBoolean() ? ChunkingSettingsTests.createRandomChunkingSettings() : null
            );
            default -> throw new IllegalArgumentException("task type " + taskType + " is not supported");
        };
    }

    protected <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response, AtomicReference<Exception> error)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }

    private static Model buildModelWithUnknownField(String inferenceEntityId) {
        return new Model(
            new ModelWithUnknownField(
                inferenceEntityId,
                TaskType.SPARSE_EMBEDDING,
                ElasticsearchInternalService.NAME,
                ElserInternalServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            )
        );
    }

    private static ServiceSettings createServiceSettings(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new TestModel.TestServiceSettings(
                "model",
                randomIntBetween(2, 100),
                randomFrom(SimilarityMeasure.values()),
                DenseVectorFieldMapper.ElementType.FLOAT
            );
            default -> new TestModelOfAnyKind.TestModelServiceSettings();
        };
    }

    public static Model createModel(String inferenceEntityId, TaskType taskType, String service) {
        var serviceSettings = createServiceSettings(taskType);
        return new Model(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings));
    }

    public static Model createModelWithSecrets(String inferenceEntityId, TaskType taskType, String service, String secret) {
        var serviceSettings = createServiceSettings(taskType);
        return new Model(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings),
            new ModelSecrets(new TestModelOfAnyKind.TestSecretSettings(secret))
        );
    }

    private static class TestModelOfAnyKind extends ModelConfigurations {

        record TestModelServiceSettings() implements ServiceSettings {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.endObject();
                return builder;
            }

            @Override
            public String getWriteableName() {
                return "test_service_settings";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.current();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public String modelId() {
                return null;
            }

            @Override
            public ToXContentObject getFilteredXContentObject() {
                return this;
            }
        }

        record TestTaskSettings() implements TaskSettings {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.endObject();
                return builder;
            }

            @Override
            public String getWriteableName() {
                return "test_task_settings";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.current();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
                return this;
            }
        }

        record TestSecretSettings(String key) implements SecretSettings {
            @Override
            public String getWriteableName() {
                return "test_secrets";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.current();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(key);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("secret", key);
                builder.endObject();
                return builder;
            }

            @Override
            public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
                return new TestSecretSettings(newSecrets.get("secret").toString());
            }
        }

        TestModelOfAnyKind(String inferenceEntityId, TaskType taskType, String service) {
            super(inferenceEntityId, taskType, service, new TestModelServiceSettings(), new TestTaskSettings());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("unknown_field", "foo");
            builder.field(INDEX_ONLY_ID_FIELD_NAME, getInferenceEntityId());
            builder.field(TaskType.NAME, getTaskType().toString());
            builder.field(SERVICE, getService());
            builder.field(SERVICE_SETTINGS, getServiceSettings());
            builder.field(TASK_SETTINGS, getTaskSettings());
            builder.endObject();
            return builder;
        }
    }

    private static class ModelWithUnknownField extends ModelConfigurations {

        ModelWithUnknownField(
            String inferenceEntityId,
            TaskType taskType,
            String service,
            ServiceSettings serviceSettings,
            TaskSettings taskSettings
        ) {
            super(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("unknown_field", "foo");
            builder.field(INDEX_ONLY_ID_FIELD_NAME, getInferenceEntityId());
            builder.field(TaskType.NAME, getTaskType().toString());
            builder.field(SERVICE, getService());
            builder.field(SERVICE_SETTINGS, getServiceSettings());
            builder.field(TASK_SETTINGS, getTaskSettings());
            builder.endObject();
            return builder;
        }
    }
}
