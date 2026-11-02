/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The named questions answered by the {@link TaskType#DECISION} task type, keyed by question name. The answers returned for a decision
 * request use the same names. Questions are provider-agnostic: services embed them in their task settings under {@link #QUESTIONS_FIELD},
 * so they can be stored on the inference endpoint and replaced as a whole through the {@code task_settings} of a single request:
 * <pre>
 * "questions": {
 *   "is_complaint": {"type": "predicate", "instructions": "Is the customer complaining?"},
 *   "team": {
 *     "type": "choice",
 *     "instructions": "Which team should handle this?",
 *     "choices": {"billing": "Charges, refunds, invoices", "other": null}
 *   }
 * }</pre>
 *
 * @param questions The questions keyed by name. The iteration order is preserved.
 */
public record DecisionQuestions(Map<String, DecisionQuestion> questions) implements Writeable, ToXContentObject {

    public static final String QUESTIONS_FIELD = "questions";

    public static final DecisionQuestions EMPTY = new DecisionQuestions(Map.of());

    public DecisionQuestions {
        Objects.requireNonNull(questions);
        for (var name : questions.keySet()) {
            if (Strings.isNullOrBlank(name)) {
                throw new IllegalArgumentException(
                    Strings.format("Field [%s] must not contain questions with an empty name", QUESTIONS_FIELD)
                );
            }
        }
        questions = Collections.unmodifiableMap(new LinkedHashMap<>(questions));
    }

    public DecisionQuestions(StreamInput in) throws IOException {
        this(in.readOrderedMap(StreamInput::readString, DecisionQuestion::readFrom));
    }

    /**
     * Parses the questions object the {@code parser} is positioned on. With {@code ignoreUnknownFields} set to {@code false} (used for
     * user requests) unknown fields within a question are rejected, with {@code true} (used for persisted endpoints) they are ignored.
     *
     * @throws IllegalArgumentException if no question is specified or a question is invalid
     */
    public static DecisionQuestions parse(XContentParser parser, boolean ignoreUnknownFields) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        var questionParser = DecisionQuestion.createParser(ignoreUnknownFields);
        var questions = new LinkedHashMap<String, DecisionQuestion>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            var name = parser.currentName();
            parser.nextToken();
            questions.put(name, questionParser.parse(parser, null));
        }
        if (questions.isEmpty()) {
            throw new IllegalArgumentException(Strings.format("Field [%s] must contain at least one question", QUESTIONS_FIELD));
        }
        return new DecisionQuestions(questions);
    }

    public boolean isEmpty() {
        return questions.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(questions, StreamOutput::writeString, DecisionQuestion::writeWithType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (var question : questions.entrySet()) {
            builder.field(question.getKey(), question.getValue());
        }
        builder.endObject();
        return builder;
    }
}
