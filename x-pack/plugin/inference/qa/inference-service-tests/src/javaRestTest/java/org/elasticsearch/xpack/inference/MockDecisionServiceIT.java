/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.DecisionResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MockDecisionServiceIT extends InferenceBaseRestTest {

    private static final String INPUT = "I was charged twice for my order.";

    public void testMockService() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);
        var model = getModels(inferenceEntityId, TaskType.DECISION).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.DECISION, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_decision_service", modelMap.get("service"));
        }

        var inference = decisionInfer(inferenceEntityId, INPUT);
        assertNonEmptyInferenceResults(inference, 2, TaskType.DECISION);
        assertEquals(inference, decisionInfer(inferenceEntityId, INPUT));
    }

    @SuppressWarnings("unchecked")
    public void testMockService_StoresQuestionsInTaskSettings() throws IOException {
        String inferenceEntityId = "test-mock-stored-questions";
        putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);
        var model = getModels(inferenceEntityId, TaskType.DECISION).get(0);

        var taskSettings = (Map<String, Object>) model.get("task_settings");
        var questions = (Map<String, Object>) taskSettings.get("questions");
        assertThat(questions, aMapWithSize(2));
        assertThat(questions.get("is_complaint"), is(Map.of("type", "predicate", "instructions", "Is the customer complaining?")));
        var team = (Map<String, Object>) questions.get("team");
        assertThat(team.get("type"), is("choice"));
        assertThat(((Map<String, Object>) team.get("choices")).keySet(), is(Set.of("billing", "shipping", "other")));
    }

    @SuppressWarnings("unchecked")
    public void testMockService_AnswersStoredQuestions() throws IOException {
        String inferenceEntityId = "test-mock-answers";
        putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);

        var inference = decisionInfer(inferenceEntityId, INPUT);

        var answers = answers(inference);
        assertThat(answers, aMapWithSize(2));

        var isComplaint = (Map<String, Object>) answers.get("is_complaint");
        assertThat(isComplaint.get(DecisionResults.Answer.TYPE), is("predicate"));
        assertThat(isComplaint.get(DecisionResults.Answer.PROBABILITY), notNullValue());

        var team = (Map<String, Object>) answers.get("team");
        assertThat(team.get(DecisionResults.Answer.TYPE), is("choice"));
        assertThat(team.get(DecisionResults.Answer.CHOICE), is(in(List.of("billing", "shipping", "other"))));
        assertThat((List<Map<String, Object>>) team.get(DecisionResults.Answer.PROBABILITIES), hasSize(3));
        assertThat(team.get(DecisionResults.Answer.CONFIDENCE), notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testMockService_RequestQuestionsReplaceStoredQuestions() throws IOException {
        String inferenceEntityId = "test-mock-override";
        putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);

        var inference = decisionInfer(
            inferenceEntityId,
            INPUT,
            Map.of(
                "questions",
                Map.of(
                    "is_urgent",
                    Map.of("type", "predicate", "instructions", "Does this need a response within the hour?"),
                    "anger",
                    Map.of("type", "score", "instructions", "How angry is the customer?", "levels", List.of("calm", "annoyed", "furious"))
                )
            )
        );

        var answers = answers(inference);
        assertThat(answers.keySet(), is(Set.of("is_urgent", "anger")));
        assertThat(((Map<String, Object>) answers.get("is_urgent")).get(DecisionResults.Answer.TYPE), is("predicate"));
        var anger = (Map<String, Object>) answers.get("anger");
        assertThat(anger.get(DecisionResults.Answer.TYPE), is("score"));
        assertThat(anger.get(DecisionResults.Answer.SCORE), notNullValue());
        var angerProbabilities = (List<Map<String, Object>>) anger.get(DecisionResults.Answer.PROBABILITIES);
        assertThat(angerProbabilities, hasSize(3));
        assertThat(angerProbabilities.get(2).get(DecisionResults.Answer.VALUE), is(2));
    }

    public void testMockService_WithoutQuestions_Fails() throws IOException {
        String inferenceEntityId = "test-mock-no-questions";
        putModel(inferenceEntityId, """
            {
              "service": "test_decision_service",
              "service_settings": {
                 "model_id": "my_model",
                 "api_key": "abc64"
              }
            }
            """, TaskType.DECISION);

        var exception = expectThrows(ResponseException.class, () -> decisionInfer(inferenceEntityId, INPUT));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(exception.getMessage(), containsString("No questions to answer"));
    }

    public void testMockService_WithInvalidRequestQuestion_Fails() throws IOException {
        String inferenceEntityId = "test-mock-invalid-request-question";
        putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);

        var exception = expectThrows(
            ResponseException.class,
            () -> decisionInfer(
                inferenceEntityId,
                INPUT,
                Map.of("questions", Map.of("team", Map.of("type", "choice", "instructions", "Which team should handle this?")))
            )
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(exception.getMessage(), containsString("Field [choices] is required for questions of type [choice]"));
    }

    public void testMockService_WithInvalidStoredQuestion_FailsToCreateEndpoint() {
        var exception = expectThrows(ResponseException.class, () -> putModel("test-mock-invalid-stored-question", """
            {
              "service": "test_decision_service",
              "service_settings": {
                 "model_id": "my_model",
                 "api_key": "abc64"
              },
              "task_settings": {
                "questions": {
                  "anger": {"type": "score", "instructions": "How angry is the customer?", "levels": ["furious"]}
                }
              }
            }
            """, TaskType.DECISION));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(exception.getMessage(), containsString("Field [levels] must contain at least [2] levels, but found [1]"));
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockDecisionServiceModelConfig(), TaskType.DECISION);
        var model = getModels(inferenceEntityId, TaskType.DECISION).get(0);

        var serviceSettings = (Map<String, Object>) model.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model_id"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model_id"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> answers(Map<String, Object> inference) {
        var decision = (Map<String, Object>) inference.get(DecisionResults.DECISION);
        return (Map<String, Object>) decision.get(DecisionResults.ANSWERS);
    }
}
