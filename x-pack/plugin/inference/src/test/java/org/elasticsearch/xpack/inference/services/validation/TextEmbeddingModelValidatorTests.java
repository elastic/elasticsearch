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
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResultsTests;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.EmptyTaskSettingsTests;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;
import org.junit.Before;
import org.mockito.Mock;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TextEmbeddingModelValidatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    @Mock
    private ServiceIntegrationValidator mockServiceIntegrationValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<Model> mockActionListener;
    @Mock
    private ServiceSettings mockServiceSettings;

    private TextEmbeddingModelValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new TextEmbeddingModelValidator(mockServiceIntegrationValidator);

        when(mockInferenceService.updateModelWithEmbeddingDetails(eq(mockModel), anyInt())).thenReturn(mockModel);
        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
        when(mockModel.getServiceSettings()).thenReturn(mockServiceSettings);
        when(mockModel.getInferenceEntityId()).thenReturn(randomAlphaOfLength(10));
    }

    public void testValidate_ServiceIntegrationValidatorThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockServiceIntegrationValidator)
            .validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener); }
        );

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_ServiceReturnsNullResults() {
        mockCallToServiceIntegrationValidator(null);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_ServiceReturnsNonTextEmbeddingResults() {
        mockCallToServiceIntegrationValidator(SparseEmbeddingResultsTests.createRandomResults());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_RetrievingEmbeddingSizeThrowsIllegalStateException() {
        TextEmbeddingFloatResults results = new TextEmbeddingFloatResults(List.of());

        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(true);
        when(mockServiceSettings.dimensions()).thenReturn(randomNonNegativeInt());

        mockCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockModel, times(1)).getServiceSettings();
        verify(mockServiceSettings).dimensions();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_DimensionsSetByUserDoNotEqualEmbeddingSize() {
        TextEmbeddingByteResults results = TextEmbeddingByteResultsTests.createRandomResults();
        var dimensions = randomValueOtherThan(results.getFirstEmbeddingSize(), ESTestCase::randomNonNegativeInt);

        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(true);
        when(mockServiceSettings.dimensions()).thenReturn(dimensions);

        mockCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockModel).getServiceSettings();
        verify(mockModel).getInferenceEntityId();
        verify(mockServiceSettings).dimensionsSetByUser();
        verify(mockServiceSettings, times(2)).dimensions();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_DimensionsSetByUserEqualEmbeddingSize() {
        mockSuccessfulValidation(true);
    }

    public void testValidate_DimensionsNotSetByUser() {
        mockSuccessfulValidation(false);
    }

    private void mockSuccessfulValidation(Boolean dimensionsSetByUser) {
        TextEmbeddingByteResults results = TextEmbeddingByteResultsTests.createRandomResults();
        when(mockModel.getConfigurations()).thenReturn(ModelConfigurationsTests.createRandomInstance());
        when(mockModel.getTaskSettings()).thenReturn(EmptyTaskSettingsTests.createRandom());
        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(dimensionsSetByUser);
        when(mockServiceSettings.dimensions()).thenReturn(dimensionsSetByUser ? results.getFirstEmbeddingSize() : null);

        mockCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onResponse(mockModel);
        verify(mockModel).getServiceSettings();
        verify(mockServiceSettings).dimensionsSetByUser();
        verify(mockServiceSettings).dimensions();
        verify(mockInferenceService).updateModelWithEmbeddingDetails(mockModel, results.getFirstEmbeddingSize());
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    private void mockCallToServiceIntegrationValidator(InferenceServiceResults results) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(results);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), eq(TIMEOUT), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
    }
}
