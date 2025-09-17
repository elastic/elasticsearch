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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class SimpleServiceIntegrationValidatorTests extends ESTestCase {

    private static final List<String> TEST_INPUT = List.of("how big");
    private static final String TEST_QUERY = "test query";
    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<InferenceServiceResults> mockActionListener;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;

    private SimpleServiceIntegrationValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new SimpleServiceIntegrationValidator();

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceThrowsException() {
        when(mockModel.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        doThrow(ElasticsearchStatusException.class).when(mockInferenceService)
            .infer(
                eq(mockModel),
                eq(null),
                eq(null),
                eq(null),
                eq(TEST_INPUT),
                eq(false),
                eq(Map.of()),
                eq(InputType.INTERNAL_INGEST),
                eq(TIMEOUT),
                any()
            );

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener); }
        );

        verifyCallToService(false);
    }

    public void testValidate_SuccessfulCallToServiceForNonReRankTaskType() {
        when(mockModel.getTaskType()).thenReturn(randomValueOtherThan(TaskType.RERANK, () -> randomFrom(TaskType.values())));

        mockSuccessfulCallToService(null, mockInferenceServiceResults);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService(false);
    }

    public void testValidate_SuccessfulCallToServiceForReRankTaskType() {
        when(mockModel.getTaskType()).thenReturn(TaskType.RERANK);

        mockSuccessfulCallToService(TEST_QUERY, mockInferenceServiceResults);
        verify(mockActionListener).onResponse(mockInferenceServiceResults);
        verifyCallToService(true);
    }

    private void mockSuccessfulCallToService(String query, InferenceServiceResults result) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(9);
            responseListener.onResponse(result);
            return null;
        }).when(mockInferenceService)
            .infer(
                eq(mockModel),
                eq(query),
                eq(null),
                eq(null),
                eq(TEST_INPUT),
                eq(false),
                eq(Map.of()),
                eq(InputType.INTERNAL_INGEST),
                eq(TIMEOUT),
                any()
            );

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void verifyCallToService(boolean withQuery) {
        verify(mockModel).getTaskType();
        verify(mockInferenceService).infer(
            eq(mockModel),
            eq(withQuery ? TEST_QUERY : null),
            eq(null),
            eq(null),
            eq(TEST_INPUT),
            eq(false),
            eq(Map.of()),
            eq(InputType.INTERNAL_INGEST),
            eq(TIMEOUT),
            any()
        );
        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }
}
