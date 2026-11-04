/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.DecisionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceDecisionResponseEntityTests extends ESTestCase {

    public void testFromResponse_ParsesAllAnswerTypes() throws IOException {
        String responseJson = """
            {
                "answers": [
                    {"type": "predicate", "name": "is_complaint", "probability": 0.92},
                    {
                        "type": "choice",
                        "name": "team",
                        "choice": "billing",
                        "confidence": 0.93,
                        "probabilities": [
                            {"value": "billing", "probability": 0.95},
                            {"value": "other", "probability": 0.05}
                        ]
                    },
                    {
                        "type": "score",
                        "name": "anger",
                        "score": 1.1,
                        "confidence": 0.55,
                        "probabilities": [
                            {"value": 0, "label": "calm", "probability": 0.1},
                            {"value": 1, "label": "annoyed", "probability": 0.7},
                            {"value": 2, "label": "furious", "probability": 0.2}
                        ]
                    }
                ]
            }
            """;

        var results = parse(responseJson);

        var expectedChoiceProbabilities = new LinkedHashMap<String, Double>();
        expectedChoiceProbabilities.put("billing", 0.95);
        expectedChoiceProbabilities.put("other", 0.05);
        var expectedScoreProbabilities = new LinkedHashMap<String, Double>();
        expectedScoreProbabilities.put("calm", 0.1);
        expectedScoreProbabilities.put("annoyed", 0.7);
        expectedScoreProbabilities.put("furious", 0.2);

        assertThat(results.answers().keySet(), contains("is_complaint", "team", "anger"));
        assertThat(results.answers().get("is_complaint"), is(new DecisionResults.PredicateAnswer(0.92)));
        assertThat(results.answers().get("team"), is(new DecisionResults.ChoiceAnswer("billing", expectedChoiceProbabilities, 0.93)));
        assertThat(results.answers().get("anger"), is(new DecisionResults.ScoreAnswer(1.1, expectedScoreProbabilities, 0.55)));
        assertThat(
            ((DecisionResults.ScoreAnswer) results.answers().get("anger")).probabilities().keySet(),
            contains("calm", "annoyed", "furious")
        );
    }

    public void testFromResponse_IgnoresUnknownFields() throws IOException {
        String responseJson = """
            {
                "id": "decision_123",
                "answers": [{"type": "predicate", "name": "is_complaint", "probability": 0.5, "extra": true}],
                "usage": {"total_tokens": 12, "modalities": {"text": {"total_tokens": 12}}}
            }
            """;

        var results = parse(responseJson);

        assertThat(results.answers(), is(Map.of("is_complaint", new DecisionResults.PredicateAnswer(0.5))));
    }

    public void testFromResponse_EmptyAnswers() throws IOException {
        var results = parse("""
            {"answers": []}
            """);

        assertThat(results.answers(), is(Map.of()));
    }

    public void testFromResponse_UnknownAnswerType_Throws() {
        String responseJson = """
            {"answers": [{"type": "ranking", "name": "order"}]}
            """;

        var exception = expectThrows(IllegalArgumentException.class, () -> parse(responseJson));
        assertThat(exception.getMessage(), is("Unknown type [ranking] for the answer to question [order]"));
    }

    public void testFromResponse_MissingRequiredField_Throws() {
        for (var answerJson : List.of("""
            {"type": "predicate", "name": "q"}""", """
            {"type": "choice", "name": "q", "confidence": 0.5, "probabilities": []}""", """
            {"type": "score", "name": "q", "score": 0.5, "probabilities": []}""", """
            {"type": "score", "name": "q", "score": 0.5, "confidence": 0.5, "probabilities": [{"value": 0, "probability": 1.0}]}""")) {
            var exception = expectThrows(IllegalArgumentException.class, () -> parse(Strings.format("""
                {"answers": [%s]}
                """, answerJson)));
            assertThat(exception.getMessage(), containsString("is missing for the"));
        }
    }

    public void testFromResponse_ScoreProbabilities_AreOrderedByValue() throws IOException {
        var results = parse("""
            {"answers": [{
                "type": "score", "name": "anger", "score": 1.0, "confidence": 0.5,
                "probabilities": [
                    {"value": 2, "label": "furious", "probability": 0.2},
                    {"value": 0, "label": "calm", "probability": 0.1},
                    {"value": 1, "label": "annoyed", "probability": 0.7}
                ]
            }]}
            """);

        var answer = (DecisionResults.ScoreAnswer) results.answers().get("anger");
        assertThat(answer.probabilities().keySet(), contains("calm", "annoyed", "furious"));
    }

    public void testFromResponse_ScoreProbabilityValueIsNotAnInteger_Throws() {
        for (var value : List.of("\"0\"", "0.5", "-1", "true")) {
            var exception = expectThrows(IllegalArgumentException.class, () -> parse(Strings.format("""
                {"answers": [{
                    "type": "score", "name": "q", "score": 0.5, "confidence": 0.5,
                    "probabilities": [{"value": %s, "label": "low", "probability": 1.0}]
                }]}
                """, value)));
            assertThat(
                exception.getMessage(),
                containsString("Field [probabilities.value] must be a non-negative integer for the [score] answer to question [q]")
            );
        }
    }

    public void testFromResponse_ScoreProbabilityWithDuplicateValue_Throws() {
        var exception = expectThrows(IllegalArgumentException.class, () -> parse("""
            {"answers": [{
                "type": "score", "name": "q", "score": 0.5, "confidence": 0.5,
                "probabilities": [
                    {"value": 0, "label": "low", "probability": 0.5},
                    {"value": 0, "label": "high", "probability": 0.5}
                ]
            }]}
            """));
        assertThat(exception.getMessage(), containsString("Duplicate probability [value] [0] for the [score] answer to question [q]"));
    }

    public void testFromResponse_ChoiceProbabilityValueIsNotAString_Throws() {
        var exception = expectThrows(IllegalArgumentException.class, () -> parse("""
            {"answers": [{
                "type": "choice", "name": "q", "choice": "a", "confidence": 0.5,
                "probabilities": [{"value": 0, "probability": 1.0}]
            }]}
            """));
        assertThat(
            exception.getMessage(),
            containsString("Field [probabilities.value] must be a string for the [choice] answer to question [q]")
        );
    }

    public void testFromResponse_MissingAnswers_Throws() {
        var exception = expectThrows(IllegalArgumentException.class, () -> parse("{}"));
        assertThat(exception.getMessage(), containsString("Required [answers]"));
    }

    private static DecisionResults parse(String json) throws IOException {
        return (DecisionResults) ElasticInferenceServiceDecisionResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), json.getBytes(StandardCharsets.UTF_8))
        );
    }
}
