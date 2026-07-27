/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class RerankServiceIntegrationValidatorTests extends ESTestCase {

    private static final List<String> TEST_INPUT = List.of("how big");
    private static final String TEST_QUERY = "test query";

    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<InferenceServiceResults> mockActionListener;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;

    private RerankServiceIntegrationValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new RerankServiceIntegrationValidator();

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockInferenceService)
            .rerankInfer(
                eq(mockModel),
                eq(new RerankRequest(InferenceString.fromStringList(TEST_INPUT), InferenceString.ofText(TEST_QUERY), null, null, null)),
                eq(TEST_REQUEST_TIMEOUT),
                any()
            );

        assertThrows(
            ElasticsearchStatusException.class,
            () -> underTest.validate(mockInferenceService, mockModel, TEST_REQUEST_TIMEOUT, mockActionListener)
        );

        verifyCallToService();
    }

    public void testValidate_SuccessfulCallToService() {
        mockSuccessfulCallToService(mockInferenceServiceResults);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService();
    }

    private void mockSuccessfulCallToService(InferenceServiceResults result) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(result);
            return null;
        }).when(mockInferenceService)
            .rerankInfer(
                eq(mockModel),
                eq(new RerankRequest(InferenceString.fromStringList(TEST_INPUT), InferenceString.ofText(TEST_QUERY), null, null, null)),
                eq(TEST_REQUEST_TIMEOUT),
                any()
            );

        underTest.validate(mockInferenceService, mockModel, TEST_REQUEST_TIMEOUT, mockActionListener);
    }

    private void verifyCallToService() {
        verify(mockInferenceService).rerankInfer(
            eq(mockModel),
            eq(new RerankRequest(InferenceString.fromStringList(TEST_INPUT), InferenceString.ofText(TEST_QUERY), null, null, null)),
            eq(TEST_REQUEST_TIMEOUT),
            any()
        );
        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }
}
