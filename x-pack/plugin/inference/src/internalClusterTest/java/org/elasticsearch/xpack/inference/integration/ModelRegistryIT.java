/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalModel;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalService;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceTests;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettingsTests;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class ModelRegistryIT extends ESSingleNodeTestCase {

    private ModelRegistry modelRegistry;

    @Before
    public void createComponents() {
        modelRegistry = new ModelRegistry(client());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, InferencePlugin.class);
    }

    public void testStoreModel() throws Exception {
        String inferenceEntityId = "test-store-model";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertThat(storeModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testStoreModelWithUnknownFields() throws Exception {
        String inferenceEntityId = "test-store-model-unknown-field";
        Model model = buildModelWithUnknownField(inferenceEntityId);
        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertNull(storeModelHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException statusException = (ElasticsearchStatusException) exceptionHolder.get();
        assertThat(
            statusException.getRootCause().getMessage(),
            containsString("mapping set to strict, dynamic introduction of [unknown_field] within [_doc] is not allowed")
        );
        assertThat(exceptionHolder.get().getMessage(), containsString("Failed to store inference endpoint [" + inferenceEntityId + "]"));
    }

    public void testGetModel() throws Exception {
        String inferenceEntityId = "test-get-model";
        Model model = buildElserModelConfig(inferenceEntityId, TaskType.SPARSE_EMBEDDING);
        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
        assertThat(putModelHolder.get(), is(true));

        // now get the model
        AtomicReference<ModelRegistry.UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(modelHolder.get(), not(nullValue()));

        assertEquals(model.getConfigurations().getService(), modelHolder.get().service());

        var elserService = new ElserInternalService(new InferenceServiceExtension.InferenceServiceFactoryContext(mock(Client.class)));
        ElserInternalModel roundTripModel = elserService.parsePersistedConfigWithSecrets(
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
        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
        assertThat(putModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        putModelHolder.set(false);
        // an model with the same id exists
        blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
        assertThat(putModelHolder.get(), is(false));
        assertThat(exceptionHolder.get(), not(nullValue()));
        assertThat(
            exceptionHolder.get().getMessage(),
            containsString("Inference endpoint [test-put-trained-model-config-exists] already exists")
        );
    }

    public void testDeleteModel() throws Exception {
        // put models
        for (var id : new String[] { "model1", "model2", "model3" }) {
            Model model = buildElserModelConfig(id, TaskType.SPARSE_EMBEDDING);
            AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
            blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
            assertThat(putModelHolder.get(), is(true));
        }

        AtomicReference<Boolean> deleteResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.deleteModel("model1", listener), deleteResponseHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertTrue(deleteResponseHolder.get());

        // get should fail
        deleteResponseHolder.set(false);
        AtomicReference<ModelRegistry.UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets("model1", listener), modelHolder, exceptionHolder);

        assertThat(exceptionHolder.get(), not(nullValue()));
        assertFalse(deleteResponseHolder.get());
        assertThat(exceptionHolder.get().getMessage(), containsString("Inference endpoint not found [model1]"));
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
            AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
            assertThat(putModelHolder.get(), is(true));
        }

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<List<ModelRegistry.UnparsedModel>> modelHolder = new AtomicReference<>();
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

            blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
            assertThat(putModelHolder.get(), is(true));
            assertNull(exceptionHolder.get());
        }

        AtomicReference<List<ModelRegistry.UnparsedModel>> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getAllModels(listener), modelHolder, exceptionHolder);
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

        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        var modelWithSecrets = createModelWithSecrets(inferenceEntityId, randomFrom(TaskType.values()), service, secret);
        blockingCall(listener -> modelRegistry.storeModel(modelWithSecrets, listener), putModelHolder, exceptionHolder);
        assertThat(putModelHolder.get(), is(true));
        assertNull(exceptionHolder.get());

        AtomicReference<ModelRegistry.UnparsedModel> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getModelWithSecrets(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get().secrets().keySet(), hasSize(1));
        var secretSettings = (Map<String, Object>) modelHolder.get().secrets().get("secret_settings");
        assertThat(secretSettings.get("secret"), equalTo(secret));

        // get model without secrets
        blockingCall(listener -> modelRegistry.getModel(inferenceEntityId, listener), modelHolder, exceptionHolder);
        assertThat(modelHolder.get().secrets().keySet(), empty());
    }

    private Model buildElserModelConfig(String inferenceEntityId, TaskType taskType) {
        return ElserInternalServiceTests.randomModelConfig(inferenceEntityId, taskType);
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
                ElserInternalService.NAME,
                ElserInternalServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            )
        );
    }

    public static Model createModel(String inferenceEntityId, TaskType taskType, String service) {
        return new Model(new ModelConfigurations(inferenceEntityId, taskType, service, new TestModelOfAnyKind.TestModelServiceSettings()));
    }

    public static Model createModelWithSecrets(String inferenceEntityId, TaskType taskType, String service, String secret) {
        return new Model(
            new ModelConfigurations(inferenceEntityId, taskType, service, new TestModelOfAnyKind.TestModelServiceSettings()),
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
