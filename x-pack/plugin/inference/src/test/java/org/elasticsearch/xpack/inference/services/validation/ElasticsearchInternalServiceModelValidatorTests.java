/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandEmbeddingModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandInternalTextEmbeddingServiceSettings;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
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

    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;
    private static final String MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE =
        "Model validation failed and model deployment could not be stopped";

    @Mock
    private SimpleServiceIntegrationValidator mockServiceIntegrationValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private CustomElandEmbeddingModel mockCustomElandEmbeddingModel;
    @Mock
    private ActionListener<Model> mockActionListener;

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

        verify(mockActionListener).onResponse(mockModel);
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

        verify(mockActionListener).onResponse(mockCustomElandEmbeddingModel);
        verify(mockCustomElandEmbeddingModel).getTaskType();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ElandTextEmbeddingModelValidationThrowsException() {
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);

        doThrow(new ElasticsearchStatusException("Model Validator Exception", RestStatus.INTERNAL_SERVER_ERROR)).when(
            mockServiceIntegrationValidator
        ).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        assertThrows(ElasticsearchStatusException.class, () -> {
            underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);
        });
    }

    public void testValidate_ElandTextEmbeddingModelValidationFails() {
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockCustomElandEmbeddingModel, mockActionListener);
    }

    public void testValidate_ElandTextEmbeddingModelValidationSucceedsAndDimensionsSetByUserValid() {
        var dimensions = randomIntBetween(1, 10);
        var mockInferenceServiceResults = mock(TextEmbeddingResults.class);
        var mockUpdatedModel = mock(CustomElandEmbeddingModel.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions))).thenReturn(
            mockUpdatedModel
        );

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockInferenceService).updateModelWithEmbeddingDetails(eq(customElandEmbeddingModel), eq(dimensions));
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).onResponse(mockUpdatedModel);
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
        var mockInferenceServiceResults = mock(TextEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(
            randomValueOtherThan(dimensions, () -> randomIntBetween(1, 10))
        );
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
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
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(true, dimensions);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
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
        var mockInferenceServiceResults = mock(TextEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenReturn(dimensions);
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);

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

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).onResponse(mockUpdatedModel);
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
        var mockInferenceServiceResults = mock(TextEmbeddingResults.class);
        when(mockInferenceServiceResults.getFirstEmbeddingSize()).thenThrow(ElasticsearchStatusException.class);
        CustomElandEmbeddingModel customElandEmbeddingModel = createCustomElandEmbeddingModel(false, null);

        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, customElandEmbeddingModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), any(), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
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

    private CustomElandEmbeddingModel createCustomElandEmbeddingModel(boolean areDimensionsSetByUser, Integer dimensions) {
        var mockServiceSettings = mock(CustomElandInternalTextEmbeddingServiceSettings.class);
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
}
