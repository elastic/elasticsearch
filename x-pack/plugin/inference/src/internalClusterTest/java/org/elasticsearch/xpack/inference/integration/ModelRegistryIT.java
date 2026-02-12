/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
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
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserMlNodeTaskSettingsTests;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.OLD_DEFAULT_SETTINGS;
import static org.elasticsearch.xpack.inference.registry.ModelRegistryTests.assertMinimalServiceSettings;
import static org.elasticsearch.xpack.inference.registry.ModelRegistryTests.assertStoreModel;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class ModelRegistryIT extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry modelRegistry;

    @Before
    public void createComponents() {
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        modelRegistry.clearDefaultIds();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testStoreModel() {
        String inferenceEntityId = "test-store-model";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        assertStoreModel(modelRegistry, model);
    }

    public void testStoreModelWithUnknownFields() {
        String inferenceEntityId = "test-store-model-unknown-field";
        Model model = buildModelWithUnknownField(inferenceEntityId);
        ElasticsearchStatusException statusException = expectThrows(
            ElasticsearchStatusException.class,
            () -> assertStoreModel(modelRegistry, model)
        );
        assertThat(
            statusException.getRootCause().getMessage(),
            containsString("mapping set to strict, dynamic introduction of [unknown_field] within [_doc] is not allowed")
        );
        assertThat(statusException.getMessage(), containsString("Failed to store inference endpoint [" + inferenceEntityId + "]"));
    }

    public void testGetModel() throws Exception {
        String inferenceEntityId = "test-get-model";
        // This can return chunking settings as null
        var model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        assertStoreModel(modelRegistry, model);

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
                Settings.EMPTY,
                InferenceStatsTests.mockInferenceStats()
            )
        );

        // When we parse the persisted config, if the chunking settings were null they will be defaulted to OLD_DEFAULT_SETTINGS
        ElasticsearchInternalModel roundTripModel = (ElasticsearchInternalModel) elserService.parsePersistedConfigWithSecrets(
            modelHolder.get().inferenceEntityId(),
            modelHolder.get().taskType(),
            modelHolder.get().settings(),
            modelHolder.get().secrets()
        );

        assertElserModelsEqual(roundTripModel, model);
    }

    /**
     * Asserts that the parsed ElasticsearchInternalModel is equal to the expected ElserInternalModel, taking into account
     * when chunking settings is null in the expected model.
     * @param actualParsedModel the parsed model by the {@link ElasticsearchInternalService}
     * @param expected the expected model that was randomly generated and stored
     */
    private static void assertElserModelsEqual(ElasticsearchInternalModel actualParsedModel, ElserInternalModel expected) {
        var expectedChunkingSettings = Objects.requireNonNullElse(expected.getConfigurations().getChunkingSettings(), OLD_DEFAULT_SETTINGS);

        // Recreate the expected model with chunking settings set to the default if it was null
        var expectedModelWithChunkingSettings = new ElserInternalModel(
            expected.getInferenceEntityId(),
            expected.getTaskType(),
            expected.getConfigurations().getService(),
            expected.getServiceSettings(),
            expected.getTaskSettings(),
            expectedChunkingSettings
        );

        assertThat(actualParsedModel, equalTo(expectedModelWithChunkingSettings));
    }

    public void testStoreModelFailsWhenModelExists() {
        String inferenceEntityId = "test-put-trained-model-config-exists";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        assertStoreModel(modelRegistry, model);

        // a model with the same id exists
        var exc = expectThrows(Exception.class, () -> assertStoreModel(modelRegistry, model));
        assertThat(exc.getMessage(), containsString("Inference endpoint [test-put-trained-model-config-exists] already exists"));
    }

    public void testDeleteModel() throws Exception {
        // put models
        for (var id : new String[] { "model1", "model2", "model3" }) {
            Model model = buildElserModelConfig(id, TaskType.SPARSE_EMBEDDING);
            assertStoreModel(modelRegistry, model);
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
            assertStoreModel(modelRegistry, model);
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
            assertStoreModel(modelRegistry, model);
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
        assertStoreModel(modelRegistry, modelWithSecrets);

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
            assertStoreModel(modelRegistry, model);
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
        assertStoreModel(modelRegistry, configured1);
        assertStoreModel(modelRegistry, configured2);

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
        assertStoreModel(modelRegistry, configuredSparse);
        assertStoreModel(modelRegistry, configuredText);
        assertStoreModel(modelRegistry, configuredRerank);

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

    public void testGetUnparsedModelMap_ThrowsResourceNotFound_WhenNoHitsReturned() {
        var listener = new PlainActionFuture<UnparsedModel>();
        modelRegistry.getModelWithSecrets("1", listener);

        ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), Matchers.is("Inference endpoint not found [1]"));
    }

    public void testStoreModels_ReturnsEmptyList_WhenGivenNoModelsToStore() {
        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response, is(List.of()));
    }

    public void testStoreModels_StoresSingleInferenceEndpoint() {
        var inferenceId = "1";
        var secrets = "secret";

        var model = new TestModel(
            inferenceId,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(1));
        assertThat(response.get(0), Matchers.is(new ModelStoreResponse(inferenceId, RestStatus.CREATED, null)));

        assertMinimalServiceSettings(modelRegistry, model);

        var listener = new PlainActionFuture<UnparsedModel>();
        modelRegistry.getModelWithSecrets(inferenceId, listener);

        var returnedModel = listener.actionGet(TIMEOUT);
        assertModel(returnedModel, model, secrets);
    }

    public void testStoreModels_StoresMultipleInferenceEndpoints() {
        var secrets = "secret";
        var inferenceId1 = "1";
        var inferenceId2 = "2";

        var model1 = new TestModel(
            inferenceId1,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        var model2 = new TestModel(
            inferenceId2,
            TaskType.TEXT_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings("model", 123, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model1, model2), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(2));
        assertThat(response.get(0), Matchers.is(new ModelStoreResponse(inferenceId1, RestStatus.CREATED, null)));
        assertThat(response.get(1), Matchers.is(new ModelStoreResponse(inferenceId2, RestStatus.CREATED, null)));

        assertModelAndMinimalSettingsWithSecrets(modelRegistry, model1, secrets);
        assertModelAndMinimalSettingsWithSecrets(modelRegistry, model2, secrets);
    }

    private static void assertModelAndMinimalSettingsWithoutSecrets(ModelRegistry registry, Model model) {
        assertMinimalServiceSettings(registry, model);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModel(model.getInferenceEntityId(), listener);

        var storedModel = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertModelWithoutSecrets(storedModel, model);
    }

    private static void assertModelAndMinimalSettingsWithSecrets(ModelRegistry registry, Model model, String secrets) {
        assertMinimalServiceSettings(registry, model);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets(model.getInferenceEntityId(), listener);

        var storedModel = listener.actionGet(TIMEOUT);
        assertModel(storedModel, model, secrets);
    }

    private static void assertModel(UnparsedModel model, Model expected, String secrets) {
        assertModelWithoutSecrets(model, expected);
        assertThat(model.secrets().keySet(), hasSize(1));
        assertThat(model.secrets().get("secret_settings"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        var secretSettings = (Map<String, Object>) model.secrets().get("secret_settings");
        assertThat(secretSettings.get("api_key"), Matchers.is(secrets));
    }

    private static void assertModelWithoutSecrets(UnparsedModel model, Model expected) {
        assertThat(model.inferenceEntityId(), Matchers.is(expected.getInferenceEntityId()));
        assertThat(model.service(), Matchers.is(expected.getConfigurations().getService()));
        assertThat(model.taskType(), Matchers.is(expected.getConfigurations().getTaskType()));
    }

    public void testStoreModels_StoresOneModel_FailsToStoreSecond_WhenVersionConflictExists() {
        var secrets = "secret";

        var inferenceId = "1";

        var model1 = new TestModel(
            inferenceId,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        var model2 = new TestModel(
            // using the same inference id as model1 to cause a failure
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings("model", 123, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model1, model2), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(2));
        assertThat(response.get(0), Matchers.is(new ModelStoreResponse(inferenceId, RestStatus.CREATED, null)));
        assertThat(response.get(1).inferenceId(), Matchers.is(model2.getInferenceEntityId()));
        assertThat(response.get(1).status(), Matchers.is(RestStatus.CONFLICT));
        assertTrue(response.get(1).failed());

        var cause = response.get(1).failureCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_1]: version conflict, document already exists"));

        assertModelAndMinimalSettingsWithSecrets(modelRegistry, model1, secrets);
        assertIndicesContainExpectedDocsCount(model1, 2);
    }

    public void testStoreModels_StoresOneModel_RemovesSecondDuplicateModelFromList_DoesNotThrowException() {
        var secrets = "secret";
        var inferenceId = "1";
        var temperature = randomInt(3);

        var model1 = new TestModel(
            inferenceId,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(temperature),
            new TestModel.TestSecretSettings(secrets)
        );

        var model2 = new TestModel(
            inferenceId,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(temperature),
            new TestModel.TestSecretSettings(secrets)
        );

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model1, model1, model2), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(1));
        assertThat(response.get(0), Matchers.is(new ModelStoreResponse(inferenceId, RestStatus.CREATED, null)));

        assertModelAndMinimalSettingsWithSecrets(modelRegistry, model1, secrets);
        assertIndicesContainExpectedDocsCount(model1, 2);
    }

    public void testStoreModels_FailsToStoreModel_WhenInferenceIndexDocumentAlreadyExists() {
        var secrets = "secret";

        var model = new TestModel(
            "1",
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        storeCorruptedModel(model, false);

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(1));
        assertThat(response.get(0).inferenceId(), Matchers.is(model.getInferenceEntityId()));
        assertThat(response.get(0).status(), Matchers.is(RestStatus.CONFLICT));
        assertTrue(response.get(0).failed());

        var cause = response.get(0).failureCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_1]: version conflict, document already exists"));
        // Since there was a partial write, both documents should be removed
        assertIndicesContainExpectedDocsCount(model, 0);
    }

    public void testStoreModels_OnFailure_RemovesPartialWritesOfInferenceEndpoint() {
        var secrets = "secret";

        var inferenceId1 = "1";
        var inferenceId2 = "2";
        var inferenceId3 = "3";

        var model1 = new TestModel(
            inferenceId1,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        var model2 = new TestModel(
            inferenceId2,
            TaskType.CHAT_COMPLETION,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        var model3 = new TestModel(
            inferenceId3,
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(secrets)
        );

        storeCorruptedModel(model1, false);
        storeCorruptedModel(model2, true);

        PlainActionFuture<List<ModelStoreResponse>> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModels(List.of(model1, model2, model3), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), Matchers.is(3));
        assertThat(response.get(0).inferenceId(), Matchers.is(model1.getInferenceEntityId()));
        assertThat(response.get(0).status(), Matchers.is(RestStatus.CONFLICT));
        assertTrue(response.get(0).failed());

        var cause = response.get(0).failureCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_1]: version conflict, document already exists"));

        // Since we did a partial write of model1's secrets, both documents should be removed
        assertIndicesContainExpectedDocsCount(model1, 0);

        assertThat(response.get(1).inferenceId(), Matchers.is(model2.getInferenceEntityId()));
        assertThat(response.get(1).status(), Matchers.is(RestStatus.CONFLICT));
        assertTrue(response.get(1).failed());

        cause = response.get(1).failureCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_2]: version conflict, document already exists"));

        // Since we did a partial write of model2's configurations, both documents should be removed
        assertIndicesContainExpectedDocsCount(model2, 0);

        // model3 should be stored successfully
        assertModelAndMinimalSettingsWithSecrets(modelRegistry, model3, secrets);
        assertIndicesContainExpectedDocsCount(model3, 2);
    }

    public void testStoreModels_Adds_OutOfSyncEndpoints_ToClusterState() {
        var inferenceId1 = "1";

        var model = new ElasticInferenceServiceSparseEmbeddingsModel(
            inferenceId1,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings("model", null, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents("url"),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );

        storeModelDirectlyInIndexWithoutRegistry(model);

        assertThat(modelRegistry.getInferenceIds(), not(hasItem(inferenceId1)));

        var storeListener = new PlainActionFuture<List<ModelStoreResponse>>();
        modelRegistry.storeModels(List.of(model), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), is(1));
        assertThat(response.get(0).inferenceId(), is(model.getInferenceEntityId()));
        assertThat(response.get(0).status(), is(RestStatus.CONFLICT));
        assertTrue(response.get(0).failed());

        // Storing the model fails because it already exists, but the registry should now be aware of the inference id in
        // cluster state
        var cause = response.get(0).failureCause();
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_1]: version conflict, document already exists"));

        assertIndicesContainExpectedDocsCount(model, 2);
        assertMinimalServiceSettings(modelRegistry, model);

        var getModelWithSecretsListener = new PlainActionFuture<UnparsedModel>();
        modelRegistry.getModelWithSecrets(model.getInferenceEntityId(), getModelWithSecretsListener);

        var unparsedModel = getModelWithSecretsListener.actionGet(TimeValue.THIRTY_SECONDS);

        assertThat(unparsedModel.inferenceEntityId(), is(model.getInferenceEntityId()));
        assertThat(unparsedModel.service(), is(model.getConfigurations().getService()));
        assertThat(unparsedModel.taskType(), is(model.getConfigurations().getTaskType()));

        assertThat(modelRegistry.getInferenceIds(), hasItem(inferenceId1));
    }

    private void storeModelDirectlyInIndexWithoutRegistry(Model model) {
        var listener = new PlainActionFuture<BulkResponse>();

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(
                ModelRegistry.createIndexRequestBuilder(
                    model.getInferenceEntityId(),
                    InferenceIndex.INDEX_NAME,
                    model.getConfigurations(),
                    false,
                    client()
                )
            )
            .add(
                ModelRegistry.createIndexRequestBuilder(
                    model.getInferenceEntityId(),
                    InferenceSecretsIndex.INDEX_NAME,
                    model.getSecrets(),
                    false,
                    client()
                )
            )
            .execute(listener);

        var bulkResponse = listener.actionGet(TimeValue.THIRTY_SECONDS);
        if (bulkResponse.hasFailures()) {
            fail("Failed to store model: " + bulkResponse.buildFailureMessage());
        }
    }

    public void testStoreModels_Adds_OutOfSyncEndpoints_ToClusterState_MixedWithSuccessfulStore() {
        var inferenceId1 = "1";

        var eisModel = new ElasticInferenceServiceSparseEmbeddingsModel(
            inferenceId1,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings("model", null, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents("url"),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );

        storeModelDirectlyInIndexWithoutRegistry(eisModel);

        assertThat(modelRegistry.getInferenceIds(), not(hasItem(inferenceId1)));

        var testModelId1 = "test-1";
        var testModelId2 = "test-2";

        // Using these models because the mock inference plugin we use in this test only supports these test services and EIS
        var testModel1 = new TestSparseInferenceServiceExtension.TestSparseModel(
            testModelId1,
            new TestSparseInferenceServiceExtension.TestServiceSettings("model", "hidden_field", false)
        );

        var testModel2 = new TestSparseInferenceServiceExtension.TestSparseModel(
            testModelId2,
            new TestSparseInferenceServiceExtension.TestServiceSettings("model", "hidden_field", false)
        );

        var storeListener = new PlainActionFuture<List<ModelStoreResponse>>();
        modelRegistry.storeModels(List.of(eisModel, testModel1, testModel2), storeListener, TimeValue.THIRTY_SECONDS);

        var response = storeListener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), is(3));
        assertThat(response.get(0).inferenceId(), is(eisModel.getInferenceEntityId()));
        assertThat(response.get(0).status(), is(RestStatus.CONFLICT));
        assertTrue(response.get(0).failed());

        assertThat(response.get(1), Matchers.is(new ModelStoreResponse(testModelId1, RestStatus.CREATED, null)));
        assertThat(response.get(2), Matchers.is(new ModelStoreResponse(testModelId2, RestStatus.CREATED, null)));

        // Storing the model fails because it already exists, but the registry should now be aware of the inference id in
        // cluster state
        var cause = response.get(0).failureCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(VersionConflictEngineException.class));
        assertThat(cause.getMessage(), containsString("[model_1]: version conflict, document already exists"));

        assertIndicesContainExpectedDocsCount(eisModel, 2);
        assertMinimalServiceSettings(modelRegistry, eisModel);

        var getModelWithSecretsListener = new PlainActionFuture<UnparsedModel>();
        modelRegistry.getModelWithSecrets(eisModel.getInferenceEntityId(), getModelWithSecretsListener);

        var unparsedModel = getModelWithSecretsListener.actionGet(TimeValue.THIRTY_SECONDS);

        assertThat(unparsedModel.inferenceEntityId(), is(eisModel.getInferenceEntityId()));
        assertThat(unparsedModel.service(), is(eisModel.getConfigurations().getService()));
        assertThat(unparsedModel.taskType(), is(eisModel.getConfigurations().getTaskType()));

        assertThat(modelRegistry.getInferenceIds(), is(Set.of(inferenceId1, testModelId1, testModelId2)));

        assertModelAndMinimalSettingsWithoutSecrets(modelRegistry, testModel1);
        assertModelAndMinimalSettingsWithoutSecrets(modelRegistry, testModel2);
    }

    public void testGetModelNoSecrets() {
        var inferenceId = "1";

        assertStoreModel(
            modelRegistry,
            new TestModel(
                inferenceId,
                TaskType.SPARSE_EMBEDDING,
                "foo",
                new TestModel.TestServiceSettings(null, null, null, null),
                new TestModel.TestTaskSettings(randomInt(3)),
                new TestModel.TestSecretSettings(randomAlphaOfLength(4))
            )
        );

        var getListener = new PlainActionFuture<UnparsedModel>();
        modelRegistry.getModel(inferenceId, getListener);

        var modelConfig = getListener.actionGet(TIMEOUT);
        assertEquals(inferenceId, modelConfig.inferenceEntityId());
        assertEquals("foo", modelConfig.service());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelConfig.taskType());
        assertNotNull(modelConfig.settings().keySet());
        assertThat(modelConfig.secrets().keySet(), empty());
    }

    public void testStoreModel_ReturnsTrue_WhenNoFailuresOccur() {
        var model = TestModel.createRandomInstance();
        assertStoreModel(modelRegistry, model);
    }

    public void testStoreModel_ThrowsException_WhenFailureIsAVersionConflict() {
        var model = TestModel.createRandomInstance();
        assertStoreModel(modelRegistry, model);

        var exception = expectThrows(ResourceAlreadyExistsException.class, () -> assertStoreModel(modelRegistry, model));
        assertThat(exception.status(), Matchers.is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            Matchers.is(format("Inference endpoint [%s] already exists", model.getConfigurations().getInferenceEntityId()))
        );
    }

    public void testStoreModel_DeletesIndexDocs_WhenInferenceIndexDocumentAlreadyExists() {
        storeCorruptedModelThenStoreModel(false);
    }

    public void testStoreModel_DeletesIndexDocs_WhenInferenceSecretsIndexDocumentAlreadyExists() {
        storeCorruptedModelThenStoreModel(true);
    }

    public void testStoreModel_DoesNotDeleteIndexDocs_WhenModelAlreadyExists() {
        var model = new TestModel(
            "model-id",
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings("secret")
        );

        PlainActionFuture<Boolean> firstStoreListener = new PlainActionFuture<>();
        modelRegistry.storeModel(model, firstStoreListener, TimeValue.THIRTY_SECONDS);
        firstStoreListener.actionGet(TimeValue.THIRTY_SECONDS);

        assertIndicesContainExpectedDocsCount(model, 2);

        PlainActionFuture<Boolean> secondStoreListener = new PlainActionFuture<>();
        modelRegistry.storeModel(model, secondStoreListener, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(ResourceAlreadyExistsException.class, () -> secondStoreListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), containsString("already exists"));
        assertThat(exception.status(), Matchers.is(RestStatus.BAD_REQUEST));
        assertIndicesContainExpectedDocsCount(model, 2);
    }

    public void testContainsPreconfiguredInferenceEndpointId() {
        var preconfiguredModelId = ".elser-2-elastic";
        var preconfiguredModel = new TestModel(
            preconfiguredModelId,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings("secret")
        );

        var userModelId = "user-model-1";
        var userModel = new TestModel(
            userModelId,
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings("secret")
        );

        var listener = new PlainActionFuture<List<ModelStoreResponse>>();
        modelRegistry.storeModels(List.of(preconfiguredModel, userModel), listener, TimeValue.THIRTY_SECONDS);

        var response = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(response.size(), is(2));
        assertFalse(response.get(0).failed());
        assertFalse(response.get(1).failed());

        assertTrue(modelRegistry.containsPreconfiguredInferenceEndpointId(preconfiguredModelId));
        assertFalse(modelRegistry.containsPreconfiguredInferenceEndpointId(userModelId));

        assertThat(modelRegistry.getInferenceIds(), is(Set.of(preconfiguredModelId, userModelId)));
    }

    private void storeCorruptedModelThenStoreModel(boolean storeSecrets) {
        var model = new TestModel(
            "corrupted-model-id",
            TaskType.SPARSE_EMBEDDING,
            "foo",
            new TestModel.TestServiceSettings(null, null, null, null),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings("secret")
        );

        storeCorruptedModel(model, storeSecrets);

        assertIndicesContainExpectedDocsCount(model, 1);

        PlainActionFuture<Boolean> storeListener = new PlainActionFuture<>();
        modelRegistry.storeModel(model, storeListener, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(ResourceAlreadyExistsException.class, () -> storeListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), containsString("already exists"));
        assertThat(exception.status(), Matchers.is(RestStatus.BAD_REQUEST));

        assertIndicesContainExpectedDocsCount(model, 0);
    }

    private void assertIndicesContainExpectedDocsCount(Model model, int numberOfDocs) {
        SearchRequest modelSearch = client().prepareSearch(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(Model.documentId(model.getInferenceEntityId()))))
            .setTrackTotalHits(false)
            .request();
        SearchResponse searchResponse = client().search(modelSearch).actionGet(TimeValue.THIRTY_SECONDS);
        try {
            assertThat(searchResponse.getHits().getHits(), Matchers.arrayWithSize(numberOfDocs));
        } finally {
            searchResponse.decRef();
        }
    }

    private void storeCorruptedModel(Model model, boolean storeSecrets) {
        var listener = new PlainActionFuture<BulkResponse>();

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(
                ModelRegistry.createIndexRequestBuilder(
                    model.getInferenceEntityId(),
                    storeSecrets ? InferenceSecretsIndex.INDEX_NAME : InferenceIndex.INDEX_NAME,
                    storeSecrets ? model.getSecrets() : model.getConfigurations(),
                    false,
                    client()
                )
            )
            .execute(listener);

        var bulkResponse = listener.actionGet(TIMEOUT);
        if (bulkResponse.hasFailures()) {
            fail("Failed to store model: " + bulkResponse.buildFailureMessage());
        }
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

    static ElserInternalModel buildElserModelConfig(String inferenceEntityId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new ElserInternalModel(
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
            case TEXT_EMBEDDING, EMBEDDING -> new TestModel.TestServiceSettings(
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
