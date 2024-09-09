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
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.EmptyTaskSettingsTests;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;
import org.elasticsearch.xpack.inference.results.InferenceTextEmbeddingByteResultsTests;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.junit.Before;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class TextEmbeddingModelValidatorTests extends ESTestCase {
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

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
        when(mockModel.getServiceSettings()).thenReturn(mockServiceSettings);
    }

    public void testValidate_ServiceIntegrationValidatorThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockServiceIntegrationValidator)
            .validate(eq(mockInferenceService), eq(mockModel), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, mockActionListener); }
        );

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_ServiceReturnsNullResults() {
        verifyCallToServiceIntegrationValidator(null);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_ServiceReturnsNonTextEmbeddingResults() {
        verifyCallToServiceIntegrationValidator(SparseEmbeddingResultsTests.createRandomResults());
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_RetrievingEmbeddingSizeThrowsIllegalStateException() {
        InferenceTextEmbeddingFloatResults results = new InferenceTextEmbeddingFloatResults(List.of());

        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(true);
        when(mockServiceSettings.dimensions()).thenReturn(randomNonNegativeInt());

        verifyCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockModel, times(3)).getServiceSettings();
        verify(mockServiceSettings).dimensionsSetByUser();
        verify(mockServiceSettings, times(2)).dimensions();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_DimensionsSetByUserDoNotEqualEmbeddingSize() {
        InferenceTextEmbeddingByteResults results = InferenceTextEmbeddingByteResultsTests.createRandomResults();
        var dimensions = randomNonNegativeInt();
        while (dimensions == results.getFirstEmbeddingSize()) {
            dimensions = randomNonNegativeInt();
        }

        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(true);
        when(mockServiceSettings.dimensions()).thenReturn(dimensions);

        verifyCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onFailure(any(ElasticsearchStatusException.class));
        verify(mockModel, times(4)).getServiceSettings();
        verify(mockModel).getConfigurations();
        verify(mockServiceSettings).dimensionsSetByUser();
        verify(mockServiceSettings, times(3)).dimensions();
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    public void testValidate_NullSimilarityProvided() {
        verifySimilarityUpdatedProperly(null, SimilarityMeasure.DOT_PRODUCT);
    }

    public void testValidate_NonNullSimilarityProvided() {
        SimilarityMeasure similarityProvided = randomFrom(
            Arrays.stream(SimilarityMeasure.values())
                .filter(similarityMeasure -> similarityMeasure.equals(SimilarityMeasure.DOT_PRODUCT) == false)
                .toList()
        );
        verifySimilarityUpdatedProperly(similarityProvided, similarityProvided);
    }

    private void verifySimilarityUpdatedProperly(SimilarityMeasure similarityProvided, SimilarityMeasure updatedSimilarity) {
        InferenceTextEmbeddingByteResults results = InferenceTextEmbeddingByteResultsTests.createRandomResults();

        when(mockModel.getConfigurations()).thenReturn(ModelConfigurationsTests.createRandomInstance());
        when(mockModel.getTaskSettings()).thenReturn(EmptyTaskSettingsTests.createRandom());
        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(true);
        when(mockServiceSettings.dimensions()).thenReturn(results.getFirstEmbeddingSize());
        when(mockServiceSettings.similarity()).thenReturn(similarityProvided);

        verifyCallToServiceIntegrationValidator(results);
        verify(mockActionListener).onResponse(mockModel);
        verify(mockModel, times(5)).getServiceSettings();
        verify(mockServiceSettings).dimensionsSetByUser();
        verify(mockServiceSettings, times(2)).dimensions();
        verify(mockServiceSettings).similarity();
        verify(mockServiceSettings).setSimilarity(updatedSimilarity);
        verifyNoMoreInteractions(mockServiceIntegrationValidator, mockInferenceService, mockModel, mockActionListener, mockServiceSettings);
    }

    private void verifyCallToServiceIntegrationValidator(InferenceServiceResults results) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(2);
            responseListener.onResponse(results);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());

        underTest.validate(mockInferenceService, mockModel, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
    }

    private void verifyPostValidationException(InferenceServiceResults results, Class<? extends Exception> exceptionClass) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(2);
            responseListener.onResponse(results);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());

        assertThrows(exceptionClass, () -> { underTest.validate(mockInferenceService, mockModel, mockActionListener); });

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
    }
}
