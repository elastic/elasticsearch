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
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class ElasticsearchInternalServiceModelValidatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;
    private static final String MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE =
        "Model validation failed and model deployment could not be stopped";

    @Mock
    private ModelValidator mockModelValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<Model> mockActionListener;

    private ElasticsearchInternalServiceModelValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new ElasticsearchInternalServiceModelValidator(mockModelValidator);

        when(mockActionListener.delegateResponse(any())).thenCallRealMethod();
    }

    public void testValidate_ModelDeploymentThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener); }
        );

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelDeploymentReturnsFalse() {
        mockModelDeployment(false);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelValidatorThrowsExceptionAndModelDeploymentIsStopped() {
        mockModelDeployment(true);
        doThrow(new ElasticsearchStatusException("Model Validator Exception", RestStatus.INTERNAL_SERVER_ERROR)).when(mockModelValidator)
            .validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        mockModelStop(true);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(mockModel), any());
        verify(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateResponse(any());
        verifyMockActionListenerAfterStopModelDeployment(true);
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelValidatorThrowsExceptionAndModelDeploymentIsNotStopped() {
        mockModelDeployment(true);
        doThrow(new ElasticsearchStatusException("Model Validator Exception", RestStatus.INTERNAL_SERVER_ERROR)).when(mockModelValidator)
            .validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        mockModelStop(false);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(mockModel), any());
        verify(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateResponse(any());
        verifyMockActionListenerAfterStopModelDeployment(false);
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelValidationFailsAndModelDeploymentIsStopped() {
        mockModelDeployment(true);
        doAnswer(ans -> {
            ActionListener<Model> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        mockModelStop(true);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(mockModel), any());
        verify(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateResponse(any());
        verifyMockActionListenerAfterStopModelDeployment(true);
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelValidationFailsAndModelDeploymentIsNotStopped() {
        mockModelDeployment(true);
        doAnswer(ans -> {
            ActionListener<Model> responseListener = ans.getArgument(3);
            responseListener.onFailure(new ElasticsearchStatusException("Model validation failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        mockModelStop(false);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockInferenceService).stop(eq(mockModel), any());
        verify(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateResponse(any());
        verifyMockActionListenerAfterStopModelDeployment(false);
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    public void testValidate_ModelValidationSucceeds() {
        mockModelDeployment(true);
        mockModelStop(true);

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
        verify(mockModelValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateResponse(any());
        verifyNoMoreInteractions(mockModelValidator, mockInferenceService, mockModel, mockActionListener);
    }

    private void mockModelDeployment(boolean modelDeploymentStarted) {
        doAnswer(ans -> {
            ActionListener<Boolean> responseListener = ans.getArgument(2);
            responseListener.onResponse(modelDeploymentStarted);
            return null;
        }).when(mockInferenceService).start(eq(mockModel), eq(TIMEOUT), any());
    }

    private void mockModelStop(boolean modelDeploymentStopped) {
        if (modelDeploymentStopped) {
            doAnswer(ans -> {
                ActionListener<Void> responseListener = ans.getArgument(1);
                responseListener.onResponse(null);
                return null;
            }).when(mockInferenceService).stop(eq(mockModel), any());
        } else {
            doAnswer(ans -> {
                ActionListener<Void> responseListener = ans.getArgument(1);
                responseListener.onFailure(new ElasticsearchStatusException("Model stop failed", RestStatus.INTERNAL_SERVER_ERROR));
                return null;
            }).when(mockInferenceService).stop(eq(mockModel), any());
        }
    }

    private void verifyMockActionListenerAfterStopModelDeployment(boolean modelDeploymentStopped) {
        verify(mockInferenceService).stop(eq(mockModel), any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockActionListener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof ElasticsearchStatusException);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((ElasticsearchStatusException) exceptionCaptor.getValue()).status());

        if (modelDeploymentStopped) {
            assertFalse(exceptionCaptor.getValue().getMessage().contains(MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE));
        } else {
            assertTrue(exceptionCaptor.getValue().getMessage().contains(MODEL_VALIDATION_AND_STOP_FAILED_MESSAGE));
        }
    }
}
