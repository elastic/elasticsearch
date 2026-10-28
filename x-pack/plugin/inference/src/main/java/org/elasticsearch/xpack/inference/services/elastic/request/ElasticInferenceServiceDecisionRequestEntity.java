/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Writes the request body of the Elastic Inference Service {@code decide} API. The question types and their fields follow the
 * <a href="https://developers.openai.com/api/docs/guides/decisions">OpenAI Decisions API</a>, while the input uses the same content
 * format ({@code type}, {@code format} and {@code value}) as the other Elastic Inference Service task types:
 * <pre>
 * {
 *   "model": "my-model",
 *   "input": {"type": "text", "format": "text", "value": "I was charged twice for my order."},
 *   "questions": [
 *     {"type": "predicate", "name": "is_complaint", "instructions": "Is the customer complaining?"},
 *     {
 *       "type": "choice",
 *       "name": "team",
 *       "instructions": "Which team should handle this?",
 *       "choices": [{"value": "billing", "description": "Charges, refunds, invoices"}, {"value": "other"}]
 *     },
 *     {
 *       "type": "score",
 *       "name": "anger",
 *       "instructions": "How angry is the customer?",
 *       "levels": [{"label": "calm"}, {"label": "furious", "description": "Threatens to leave"}]
 *     }
 *   ]
 * }</pre>
 * Questions are sent as an array to preserve their order, with the question name as a field of each question.
 */
public record ElasticInferenceServiceDecisionRequestEntity(InferenceString input, DecisionQuestions questions, String modelId)
    implements
        ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String INPUT_FIELD = "input";
    private static final String QUESTIONS_FIELD = "questions";
    private static final String TYPE_FIELD = "type";
    private static final String NAME_FIELD = "name";
    private static final String INSTRUCTIONS_FIELD = "instructions";
    private static final String CHOICES_FIELD = "choices";
    private static final String VALUE_FIELD = "value";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String LEVELS_FIELD = "levels";
    private static final String LABEL_FIELD = "label";

    public ElasticInferenceServiceDecisionRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(questions);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_FIELD, modelId);
        builder.field(INPUT_FIELD, input);
        builder.startArray(QUESTIONS_FIELD);
        for (var question : questions.questions().entrySet()) {
            writeQuestion(builder, question.getKey(), question.getValue());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static void writeQuestion(XContentBuilder builder, String name, DecisionQuestion question) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD, question.type().toString());
        builder.field(NAME_FIELD, name);
        builder.field(INSTRUCTIONS_FIELD, question.instructions());
        switch (question) {
            case DecisionQuestion.Predicate ignored -> {
                // predicates only consist of the instructions
            }
            case DecisionQuestion.Choice choice -> {
                builder.startArray(CHOICES_FIELD);
                for (Map.Entry<String, String> option : choice.choices().entrySet()) {
                    builder.startObject();
                    builder.field(VALUE_FIELD, option.getKey());
                    if (option.getValue() != null) {
                        builder.field(DESCRIPTION_FIELD, option.getValue());
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            case DecisionQuestion.Score score -> {
                builder.startArray(LEVELS_FIELD);
                for (var level : score.levels()) {
                    builder.startObject();
                    builder.field(LABEL_FIELD, level.label());
                    if (level.description() != null) {
                        builder.field(DESCRIPTION_FIELD, level.description());
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
        }
        builder.endObject();
    }
}
