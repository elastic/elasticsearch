/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class SimpleChatCompletionServiceIntegrationValidatorTests extends ESTestCase {

    private static final UnifiedCompletionRequest EXPECTED_REQUEST = new UnifiedCompletionRequest(
        List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("how big"), "user", null, null)),
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<InferenceServiceResults> mockActionListener;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;

    private SimpleChatCompletionServiceIntegrationValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new SimpleChatCompletionServiceIntegrationValidator();

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockInferenceService)
            .unifiedCompletionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener)
        );

        verifyCallToService();
    }

    public void testValidate_SuccessfulCallToService() {
        mockSuccessfulCallToService(mockInferenceServiceResults);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService();
    }

    public void testValidate_CallsListenerOnFailure_WhenServiceResponseIsNull() {
        mockNullResponseFromService();

        var captor = ArgumentCaptor.forClass(ElasticsearchStatusException.class);
        verify(mockActionListener).onFailure(captor.capture());

        assertThat(
            captor.getValue().getMessage(),
            is("Could not complete inference endpoint creation as validation call to service returned null response.")
        );
        assertThat(captor.getValue().status(), is(RestStatus.BAD_REQUEST));

        verifyCallToService();
    }

    public void testValidate_CallsListenerOnFailure_WhenServiceThrowsException() {
        var returnedException = new IllegalStateException("bad state");
        mockFailureResponseFromService(returnedException);

        var captor = ArgumentCaptor.forClass(ElasticsearchStatusException.class);
        verify(mockActionListener).onFailure(captor.capture());

        assertThat(
            captor.getValue().getMessage(),
            is("Could not complete inference endpoint creation as validation call to service threw an exception.")
        );
        assertThat(captor.getValue().status(), is(RestStatus.BAD_REQUEST));
        assertThat(captor.getValue().getCause(), is(returnedException));

        verifyCallToService();
    }

    private void mockSuccessfulCallToService(InferenceServiceResults result) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(result);
            return null;
        }).when(mockInferenceService).unifiedCompletionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void mockNullResponseFromService() {
        mockSuccessfulCallToService(null);
    }

    private void mockFailureResponseFromService(Exception exception) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(exception);
            return null;
        }).when(mockInferenceService).unifiedCompletionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void verifyCallToService() {
        verify(mockInferenceService).unifiedCompletionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());
        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }
}
