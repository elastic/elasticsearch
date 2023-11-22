/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeModel;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeService;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceTests;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettingsTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
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
        String modelId = "test-store-model";
        Model model = buildModelConfig(modelId, ElserMlNodeService.NAME, TaskType.SPARSE_EMBEDDING);
        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertThat(storeModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testStoreModelWithUnknownFields() throws Exception {
        String modelId = "test-store-model-unknown-field";
        Model model = buildModelWithUnknownField(modelId);
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
        assertThat(exceptionHolder.get().getMessage(), containsString("Failed to store inference model [" + modelId + "]"));
    }

    public void testGetModel() throws Exception {
        String modelId = "test-get-model";
        Model model = buildModelConfig(modelId, ElserMlNodeService.NAME, TaskType.SPARSE_EMBEDDING);
        AtomicReference<Boolean> putModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), putModelHolder, exceptionHolder);
        assertThat(putModelHolder.get(), is(true));

        // now get the model
        AtomicReference<ModelRegistry.ModelConfigMap> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getUnparsedModelMap(modelId, listener), modelHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(modelHolder.get(), not(nullValue()));

        UnparsedModel unparsedModel = UnparsedModel.unparsedModelFromMap(modelHolder.get().config(), modelHolder.get().secrets());
        assertEquals(model.getConfigurations().getService(), unparsedModel.service());

        var elserService = new ElserMlNodeService(new InferenceServicePlugin.InferenceServiceFactoryContext(mock(Client.class)));
        ElserMlNodeModel roundTripModel = elserService.parsePersistedConfig(
            unparsedModel.modelId(),
            unparsedModel.taskType(),
            unparsedModel.settings(),
            unparsedModel.secrets()
        );
        assertEquals(model, roundTripModel);
    }

    public void testStoreModelFailsWhenModelExists() throws Exception {
        String modelId = "test-put-trained-model-config-exists";
        Model model = buildModelConfig(modelId, ElserMlNodeService.NAME, TaskType.SPARSE_EMBEDDING);
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
            containsString("Inference model [test-put-trained-model-config-exists] already exists")
        );
    }

    public void testDeleteModel() throws Exception {
        // put models
        for (var id : new String[] { "model1", "model2", "model3" }) {
            Model model = buildModelConfig(id, ElserMlNodeService.NAME, TaskType.SPARSE_EMBEDDING);
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
        AtomicReference<ModelRegistry.ModelConfigMap> modelHolder = new AtomicReference<>();
        blockingCall(listener -> modelRegistry.getUnparsedModelMap("model1", listener), modelHolder, exceptionHolder);

        assertThat(exceptionHolder.get(), not(nullValue()));
        assertFalse(deleteResponseHolder.get());
        assertThat(exceptionHolder.get().getMessage(), containsString("Model not found [model1]"));
    }

    private Model buildModelConfig(String modelId, String service, TaskType taskType) {
        return switch (service) {
            case ElserMlNodeService.NAME -> ElserMlNodeServiceTests.randomModelConfig(modelId, taskType);
            default -> throw new IllegalArgumentException("unknown service " + service);
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

    private static Model buildModelWithUnknownField(String modelId) {
        return new Model(
            new ModelWithUnknownField(
                modelId,
                TaskType.SPARSE_EMBEDDING,
                ElserMlNodeService.NAME,
                ElserMlNodeServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            )
        );
    }

    private static class ModelWithUnknownField extends ModelConfigurations {

        ModelWithUnknownField(
            String modelId,
            TaskType taskType,
            String service,
            ServiceSettings serviceSettings,
            TaskSettings taskSettings
        ) {
            super(modelId, taskType, service, serviceSettings, taskSettings);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("unknown_field", "foo");
            builder.field(MODEL_ID, getModelId());
            builder.field(TaskType.NAME, getTaskType().toString());
            builder.field(SERVICE, getService());
            builder.field(SERVICE_SETTINGS, getServiceSettings());
            builder.field(TASK_SETTINGS, getTaskSettings());
            builder.endObject();
            return builder;
        }
    }
}
