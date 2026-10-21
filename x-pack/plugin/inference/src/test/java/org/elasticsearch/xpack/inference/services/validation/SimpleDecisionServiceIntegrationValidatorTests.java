/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.DecisionRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.validation.SimpleDecisionServiceIntegrationValidator.TEST_INPUT;
import static org.elasticsearch.xpack.inference.services.validation.SimpleDecisionServiceIntegrationValidator.TEST_QUESTION_NAME;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class SimpleDecisionServiceIntegrationValidatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.ONE_MINUTE;

    private static final DecisionRequest EXPECTED_REQUEST = new DecisionRequest(
        TEST_INPUT,
        Map.of("questions", Map.of(TEST_QUESTION_NAME, Map.of("type", "predicate", "instructions", "Is the text a greeting?")))
    );

    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<InferenceServiceResults> mockActionListener;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;

    private SimpleDecisionServiceIntegrationValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new SimpleDecisionServiceIntegrationValidator();

        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
    }

    public void testValidate_ServiceThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockInferenceService)
            .decisionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

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
        mockSuccessfulCallToService(null);

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
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onFailure(returnedException);
            return null;
        }).when(mockInferenceService).decisionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);

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

    @SuppressWarnings("unchecked")
    public void testCreateRequest_ContainsValidQuestions() throws IOException {
        var questionsMap = (Map<String, Object>) SimpleDecisionServiceIntegrationValidator.createRequest()
            .taskSettings()
            .get(DecisionQuestions.QUESTIONS_FIELD);
        try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, questionsMap)) {
            parser.nextToken();
            var questions = DecisionQuestions.parse(parser, false);
            assertThat(questions.questions(), is(Map.of(TEST_QUESTION_NAME, new DecisionQuestion.Predicate("Is the text a greeting?"))));
        }
    }

    private void mockSuccessfulCallToService(InferenceServiceResults result) {
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(3);
            responseListener.onResponse(result);
            return null;
        }).when(mockInferenceService).decisionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());

        underTest.validate(mockInferenceService, mockModel, TIMEOUT, mockActionListener);
    }

    private void verifyCallToService() {
        verify(mockInferenceService).decisionInfer(eq(mockModel), eq(EXPECTED_REQUEST), eq(TIMEOUT), any());
        verifyNoMoreInteractions(mockInferenceService, mockModel, mockActionListener, mockInferenceServiceResults);
    }
}
