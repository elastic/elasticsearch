/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ModelLoadingServiceTests extends ESTestCase {

    TrainedModelProvider trainedModelProvider;

    @Before
    public void setUpComponents() {
        trainedModelProvider = mock(TrainedModelProvider.class);
    }

    public void testGetModelAndCache() throws Exception {
        String model1 = "test-load-model-1";
        String model2 = "test-load-model-2";
        String model3 = "test-load-model-3";
        withTrainedModel(model1, 0);
        withTrainedModel(model2, 0);
        withTrainedModel(model3, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider);

        String[] modelIds = new String[]{model1, model2, model3};
        for(int i = 0; i < 10; i++) {
            String model = modelIds[i%3];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModelAndCache(model, 0, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(0L), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(0L), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(0L), any());
    }

    public void testGetMissingModelAndCache() {
        String model = "test-load-missing-model";
        withMissingModel(model, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider);

        modelLoadingService.getModelAndCache(model, 0, ActionListener.wrap(
            m -> fail("Should not have succeeded"),
            f -> assertThat(f.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model, 0)))
        ));
    }

    @SuppressWarnings("unchecked")
    private void withTrainedModel(String modelId, long modelVersion) {
        TrainedModelConfig trainedModelConfig = buildTrainedModelConfigBuilder(modelId, modelVersion).build(Version.CURRENT);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(trainedModelConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(modelVersion), any());
    }

    private void withMissingModel(String modelId, long modelVersion) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onFailure(new ResourceNotFoundException(
                Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId, modelVersion)));
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(modelVersion), any());
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId, long modelVersion) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType("binary_decision_tree")
            .setModelVersion(modelVersion);
    }
}
