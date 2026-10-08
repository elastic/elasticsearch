/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandEmbeddingModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticDeployedModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalTextEmbeddingServiceSettings;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class ElasticsearchInternalServiceModelValidatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = ESTestCase.TEST_REQUEST_TIMEOUT;
    private static final String MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE =
        "Model validation failed and model deployment could not be stopped";

    @Mock
    private SimpleServiceIntegrationValidator mockServiceIntegrationValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private CustomElandEmbeddingModel mockCustomElandEmbeddingModel;
    @Mock
    private ActionListener<ModelValidationResult> mockActionListener;

    private ElasticsearchInternalServiceModelValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new ElasticsearchInternalServiceModelValidator(mockServiceIntegrationValidator);

        when(mockActionListener.delegateResponse(any())).thenCallRealMethod();
        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_NonElandModelSkipsValidation() {
        var mockModel = mock(Model.class);
        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockActionListener).onResponse(argThat(result -> result.model() == mockModel && result.deploymentStarted() == false));
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockModel
        );
    }

    public void testValidate_ElandModelWithNonTextEmbeddingTaskTypeSkipsValidation() {
        when(mockCustomElandEmbeddingModel.getTaskType()).thenReturn(randomFrom(List.of(TaskType.RERANK, TaskType.SPARSE_EMBEDDING)));

        underTest.validate(mockInferenceService, mockCustomElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockActionListener).onResponse(
            argThat(result -> result.model() == mockCustomElandEmbeddingModel && result.deploymentStarted() == false)
        );
        verify(mockCustomElandEmbeddingModel).getTaskType();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ElandTextEmbeddingModelValidationThrowsException() {
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);
        stubServiceStartSuccess();

        doThrow(new ElasticsearchStatusException("Model Validator Exception", RestStatus.INTERNAL_SERVER_ERROR)).when(
            mockServiceIntegrationValidator
        ).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ElandTextEmbeddingModelValidationFails() {
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);
        stubServiceStartSuccess();
        stubServiceStopSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(customElandEmbeddingModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ValidationFails_StopAlsoFails_WrapsException() {
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);
        stubServiceStartSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        doAnswer(ans -> {
            ActionListener<Boolean> stopListener = ans.getArgument(1);
            stopListener.onFailure(new ElasticsearchStatusException("stop failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockInferenceService).stop(any(), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(customElandEmbeddingModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(
            argThat(e -> e instanceof ElasticsearchStatusException && e.getMessage().contains(MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE))
        );
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ElandTextEmbeddingModelValidationSucceedsAndDimensionsSetByUserValid() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        var mockUpdatedModel = mock(CustomElandEmbeddingModel.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);
        stubServiceStartSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions))).thenReturn(
            mockUpdatedModel
        );

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions));
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onResponse(argThat(result -> result.model() == mockUpdatedModel && result.deploymentStarted()));
        verify(mockInferenceServiceResults).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockUpdatedModel,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElandTextEmbeddingModelValidationSucceedsAndDimensionsSetByUserInvalid() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(
            randomValueOtherThan(dimensions, () -> randomIntBetween(1, 10))
        );
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);
        stubServiceStartSuccess();
        stubServiceStopSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(customElandEmbeddingModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockInferenceServiceResults, times(2)).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElandTextEmbeddingAndValidationReturnsInvalidResultsType() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(InferenceServiceResults.class);
        when(mockInferenceServiceResults.getWriteableName()).thenReturn(randomAlphaOfLength(10));
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);
        stubServiceStartSuccess();
        stubServiceStopSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(customElandEmbeddingModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockInferenceServiceResults).getWriteableName();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElandTextEmbeddingModelDimensionsNotSetByUser() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);
        stubServiceStartSuccess();

        var mockUpdatedModel = mock(CustomElandEmbeddingModel.class);
        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions))).thenReturn(
            mockUpdatedModel
        );

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onResponse(argThat(result -> result.model() == mockUpdatedModel && result.deploymentStarted()));
        verify(mockInferenceService).updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions));
        verify(mockInferenceServiceResults).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElandTextEmbeddingModelAndEmbeddingSizeRetrievalThrowsException() {
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenThrow(ElasticsearchStatusException.class);
        var customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);
        stubServiceStartSuccess();
        stubServiceStopSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(customElandEmbeddingModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(customElandEmbeddingModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockInferenceServiceResults).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElasticDeployedModel_TextEmbedding_RoutedThroughValidation() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        var elasticDeployedModel = createElasticDeployedModel(false, null);
        stubServiceStartSuccess();

        var mockUpdatedModel = mock(ElasticDeployedModel.class);
        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(elasticDeployedModel), eq(dimensions))).thenReturn(mockUpdatedModel);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, elasticDeployedModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(elasticDeployedModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).updateModelWithEmbeddingDetails(eq(elasticDeployedModel), eq(dimensions));
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onResponse(argThat(result -> result.model() == mockUpdatedModel && result.deploymentStarted()));
        verify(mockInferenceServiceResults).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockUpdatedModel,
            mockInferenceServiceResults
        );
    }

    public void testValidate_ElasticDeployedModelWithNonTextEmbeddingTaskTypeSkipsValidation() {
        var elasticDeployedModel = createElasticDeployedModel(randomFrom(TaskType.RERANK, TaskType.SPARSE_EMBEDDING));

        underTest.validate(mockInferenceService, elasticDeployedModel, TIMEOUT, mockActionListener);

        verify(mockActionListener).onResponse(
            argThat(result -> result.model() == elasticDeployedModel && result.deploymentStarted() == false)
        );
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockActionListener);
    }

    public void testValidate_ElasticDeployedTextEmbeddingModelValidationFails() {
        var elasticDeployedModel = createElasticDeployedModel(TaskType.TEXT_EMBEDDING);
        stubServiceStartSuccess();
        stubServiceStopSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(elasticDeployedModel), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, elasticDeployedModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(elasticDeployedModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(elasticDeployedModel), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(elasticDeployedModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockActionListener);
    }

    public void testValidate_ElasticDeployedTextEmbeddingModelValidationSucceeds() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(DenseEmbeddingResults.class);
        var mockUpdatedModel = mock(ElasticDeployedModel.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        var elasticDeployedModel = createElasticDeployedModel(TaskType.TEXT_EMBEDDING);
        stubServiceStartSuccess();

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(elasticDeployedModel), eq(TIMEOUT), any());
        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(elasticDeployedModel), eq(dimensions))).thenReturn(mockUpdatedModel);

        underTest.validate(mockInferenceService, elasticDeployedModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(elasticDeployedModel), eq(TIMEOUT), any());
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(elasticDeployedModel), eq(TIMEOUT), any());
        verify(mockInferenceService).updateModelWithEmbeddingDetails(eq(elasticDeployedModel), eq(dimensions));
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).delegateResponse(any());
        verify(mockActionListener).onResponse(argThat(result -> result.model() == mockUpdatedModel && result.deploymentStarted()));
        verify(mockInferenceServiceResults).getFirstEmbeddingSize();
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockCustomElandEmbeddingModel,
            mockActionListener,
            mockUpdatedModel,
            mockInferenceServiceResults
        );
    }

    private void stubServiceStartSuccess() {
        doAnswer(ans -> {
            ActionListener<Void> startListener = ans.getArgument(2);
            startListener.onResponse(null);
            return null;
        }).when(mockInferenceService).start(any(), any(), any());
    }

    private void stubServiceStopSuccess() {
        doAnswer(ans -> {
            ActionListener<Boolean> stopListener = ans.getArgument(1);
            stopListener.onResponse(Boolean.TRUE);
            return null;
        }).when(mockInferenceService).stop(any(), any());
    }

    private ElasticDeployedModel createElasticDeployedModel(TaskType taskType) {
        var mockServiceSettings = mock(ElasticsearchInternalTextEmbeddingServiceSettings.class);
        when(mockServiceSettings.modelId()).thenReturn(randomAlphaOfLength(10));

        return new ElasticDeployedModel(
            randomAlphaOfLength(10),
            taskType,
            randomAlphaOfLength(10),
            mockServiceSettings,
            ChunkingSettingsTests.createRandomChunkingSettings()
        );
    }

    private CustomElandEmbeddingModel createCustomElandEmbeddingModel(boolean areDimensionsSetByUser, Integer dimensions) {
        var mockServiceSettings = mock(ElasticsearchInternalTextEmbeddingServiceSettings.class);
        when(mockServiceSettings.modelId()).thenReturn(randomAlphaOfLength(10));
        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(areDimensionsSetByUser);
        if (dimensions != null) {
            when(mockServiceSettings.dimensions()).thenReturn(dimensions);
        }

        return new CustomElandEmbeddingModel(
            randomAlphaOfLength(10),
            TaskType.TEXT_EMBEDDING,
            randomAlphaOfLength(10),
            mockServiceSettings,
            ChunkingSettingsTests.createRandomChunkingSettings()
        );
    }

    private ElasticDeployedModel createElasticDeployedModel(boolean areDimensionsSetByUser, Integer dimensions) {
        var mockServiceSettings = mock(ElasticsearchInternalServiceSettings.class);
        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(areDimensionsSetByUser);
        if (dimensions != null) {
            when(mockServiceSettings.dimensions()).thenReturn(dimensions);
        }

        return new ElasticDeployedModel(
            randomAlphaOfLength(10),
            TaskType.TEXT_EMBEDDING,
            randomAlphaOfLength(10),
            mockServiceSettings,
            ChunkingSettingsTests.createRandomChunkingSettings()
        );
    }
}
