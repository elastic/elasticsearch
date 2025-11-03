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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaService;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettings;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class NvidiaServiceIntegrationValidatorTests extends ESTestCase {
    private static final List<String> TEST_INPUT = List.of("how big");
    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    @Mock
    private NvidiaService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<InferenceServiceResults> mockActionListener;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;

    private NvidiaServiceIntegrationValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new NvidiaServiceIntegrationValidator();

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceThrowsException() {
        when(mockModel.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        when(mockModel.getTaskSettings()).thenReturn(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS);

        doThrow(ElasticsearchStatusException.class).when(mockInferenceService)
            .infer(eq(mockModel), eq(null), eq(null), eq(null), eq(TEST_INPUT), eq(false), eq(Map.of()), eq(null), eq(TIMEOUT), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener)
        );

        verifyCallToService(null);
    }

    public void testValidate_SuccessfulCallToServiceForTextEmbeddingsTaskTypeWithInputType() {
        when(mockModel.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        when(mockModel.getTaskSettings()).thenReturn(new NvidiaEmbeddingsTaskSettings(InputType.INTERNAL_INGEST, null));

        mockSuccessfulCallToService(mockInferenceServiceResults, InputType.INTERNAL_INGEST);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService(InputType.INTERNAL_INGEST);
    }

    public void testValidate_SuccessfulCallToServiceForTextEmbeddingsTaskTypeNoInputType() {
        when(mockModel.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        when(mockModel.getTaskSettings()).thenReturn(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS);

        mockSuccessfulCallToService(mockInferenceServiceResults, null);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService(null);
    }

    public void testValidate_NonTextEmbedding() {
        TaskType taskType = randomValueOtherThan(TaskType.TEXT_EMBEDDING, () -> randomFrom(TaskType.values()));
        when(mockModel.getTaskType()).thenReturn(taskType);
        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
        verify(mockModel, times(2)).getTaskType();
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));

        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }

    private void mockSuccessfulCallToService(InferenceServiceResults result, InputType inputType) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(9);
            responseListener.onResponse(result);
            return null;
        }).when(mockInferenceService)
            .infer(eq(mockModel), eq(null), eq(null), eq(null), eq(TEST_INPUT), eq(false), eq(Map.of()), eq(inputType), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void verifyCallToService(InputType inputType) {
        verify(mockModel).getTaskType();
        verify(mockModel).getTaskSettings();
        verify(mockInferenceService).infer(
            eq(mockModel),
            eq(null),
            eq(null),
            eq(null),
            eq(TEST_INPUT),
            eq(false),
            eq(Map.of()),
            eq(inputType),
            eq(TIMEOUT),
            any()
        );
        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }

}
