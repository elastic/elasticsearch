/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDecisionRequestEntityTests extends ESTestCase {

    public void testToXContent_TextInput_AllQuestionTypes() throws IOException {
        var choices = new LinkedHashMap<String, String>();
        choices.put("billing", "Charges, refunds, invoices");
        choices.put("other", null);
        var questions = new LinkedHashMap<String, DecisionQuestion>();
        questions.put("is_complaint", new DecisionQuestion.Predicate("Is the customer complaining?"));
        questions.put("team", new DecisionQuestion.Choice("Which team should handle this?", choices));
        questions.put(
            "anger",
            new DecisionQuestion.Score(
                "How angry is the customer?",
                List.of(
                    new DecisionQuestion.Level("calm"),
                    new DecisionQuestion.Level("annoyed"),
                    new DecisionQuestion.Level("furious", "Threatens to leave")
                )
            )
        );

        var entity = new ElasticInferenceServiceDecisionRequestEntity(
            InferenceString.ofText("I was charged twice for my order."),
            new DecisionQuestions(questions),
            "my-model"
        );

        assertThat(Strings.toString(entity), is(XContentHelper.stripWhitespace("""
            {
                "model": "my-model",
                "input": {"type": "text", "format": "text", "value": "I was charged twice for my order."},
                "questions": [
                    {"type": "predicate", "name": "is_complaint", "instructions": "Is the customer complaining?"},
                    {
                        "type": "choice",
                        "name": "team",
                        "instructions": "Which team should handle this?",
                        "choices": [{"value": "billing", "description": "Charges, refunds, invoices"}, {"value": "other"}]
                    },
                    {
                        "type": "score",
                        "name": "anger",
                        "instructions": "How angry is the customer?",
                        "levels": [{"label": "calm"}, {"label": "annoyed"}, {"label": "furious", "description": "Threatens to leave"}]
                    }
                ]
            }""")));
    }

    public void testToXContent_ImageInput_IsSentAsContentObject() throws IOException {
        var format = randomFrom(DataFormat.BASE64, DataFormat.URL);
        var value = format == DataFormat.BASE64 ? TEST_DATA_URI : "https://example.com/image.png";
        var entity = new ElasticInferenceServiceDecisionRequestEntity(
            new InferenceString(DataType.IMAGE, format, value),
            new DecisionQuestions(Map.of("visible_damage", new DecisionQuestion.Predicate("Is there visible damage?"))),
            "my-model"
        );

        assertThat(Strings.toString(entity), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model": "my-model",
                "input": {"type": "image", "format": "%s", "value": "%s"},
                "questions": [{"type": "predicate", "name": "visible_damage", "instructions": "Is there visible damage?"}]
            }""", format, value))));
    }
}
