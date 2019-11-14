/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TrainedModelProviderIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testPutTrainedModelConfig() throws Exception {
        String modelId = "test-put-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testPutTrainedModelConfigThatAlreadyExists() throws Exception {
        String modelId = "test-put-trained-model-config-exists";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, modelId)));
    }

    public void testGetTrainedModelConfig() throws Exception {
        String modelId = "test-get-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(config));
        assertThat(getConfigHolder.get().getDefinition(), is(not(nullValue())));
    }

    public void testGetTrainedModelConfigWithoutDefinition() throws Exception {
        String modelId = "test-get-trained-model-config-no-definition";
        TrainedModelConfig.Builder configBuilder = buildTrainedModelConfigBuilder(modelId);
        TrainedModelConfig config = configBuilder.build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, false, listener), getConfigHolder, exceptionHolder);
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(),
            equalTo(configBuilder.setCreateTime(config.getCreateTime()).setDefinition((TrainedModelDefinition) null).build()));
        assertThat(getConfigHolder.get().getDefinition(), is(nullValue()));
    }

    public void testGetMissingTrainingModelConfig() throws Exception {
        String modelId = "test-get-missing-trained-model-config";
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
    }

    public void testGetMissingTrainingModelConfigDefinition() throws Exception {
        String modelId = "test-get-missing-trained-model-config-definition";
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        client().delete(new DeleteRequest(InferenceIndexConstants.LATEST_INDEX_NAME)
            .id(TrainedModelDefinition.docId(config.getModelId()))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setVersion(Version.CURRENT)
            .setInput(TrainedModelInputTests.createRandomInput());
    }

    private static TrainedModelConfig buildTrainedModelConfig(String modelId) {
        return buildTrainedModelConfigBuilder(modelId).build();
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);

    }

}
