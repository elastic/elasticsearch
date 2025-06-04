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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.junit.Before;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class SimpleModelValidatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    @Mock
    private ServiceIntegrationValidator mockServiceIntegrationValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<Model> mockActionListener;

    private SimpleModelValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new SimpleModelValidator(mockServiceIntegrationValidator);

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceIntegrationValidatorThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockServiceIntegrationValidator)
            .validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener); }
        );
        verifyInteractions();
    }

    public void testValidate_ServiceReturnsInferenceServiceResults() {
        mockCallToServiceIntegrationValidator(SparseEmbeddingResultsTests.createRandomResults());
        verify(mockActionListener).onResponse(mockModel);
        verifyInteractions();
    }

    private void mockCallToServiceIntegrationValidator(InferenceServiceResults results) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(results);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void verifyInteractions() {
        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener);
    }
}
