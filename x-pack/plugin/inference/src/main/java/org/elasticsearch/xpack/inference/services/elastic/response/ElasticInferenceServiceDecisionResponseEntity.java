/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DecisionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parses the response of the Elastic Inference Service {@code decide} API. The answer types and their fields follow the
 * <a href="https://developers.openai.com/api/docs/guides/decisions">OpenAI Decisions API</a>:
 * <pre>
 * {
 *   "answers": [
 *     {"type": "predicate", "name": "is_complaint", "probability": 0.92},
 *     {
 *       "type": "choice",
 *       "name": "team",
 *       "choice": "billing",
 *       "confidence": 0.93,
 *       "probabilities": [{"value": "billing", "probability": 0.95}, {"value": "other", "probability": 0.05}]
 *     },
 *     {
 *       "type": "score",
 *       "name": "anger",
 *       "score": 0.3,
 *       "confidence": 0.55,
 *       "probabilities": [{"value": 0, "label": "calm", "probability": 0.7}, {"value": 1, "label": "furious", "probability": 0.3}]
 *     }
 *   ],
 *   "usage": {"total_tokens": 42}
 * }</pre>
 * The {@code usage} is used by the Elastic Inference Service for metering and is not part of the {@link DecisionResults}.
 * The answers are keyed by question name in the {@link DecisionResults}. Choice probabilities are keyed by the choice {@code value},
 * score probabilities by the level {@code label}, ordered by the zero-based integer level position in {@code value}.
 */
public class ElasticInferenceServiceDecisionResponseEntity {

    private static final String PREDICATE_TYPE = "predicate";
    private static final String CHOICE_TYPE = "choice";
    private static final String SCORE_TYPE = "score";

    record DecisionResponse(List<AnswerEntry> answers) {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<DecisionResponse, Void> PARSER = new ConstructingObjectParser<>(
            DecisionResponse.class.getSimpleName(),
            true,
            args -> new DecisionResponse((List<AnswerEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), AnswerEntry.PARSER::apply, new ParseField("answers"));
        }
    }

    record AnswerEntry(
        String type,
        String name,
        @Nullable Double probability,
        @Nullable String choice,
        @Nullable Double score,
        @Nullable Double confidence,
        @Nullable List<ProbabilityEntry> probabilities
    ) {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<AnswerEntry, Void> PARSER = new ConstructingObjectParser<>(
            AnswerEntry.class.getSimpleName(),
            true,
            args -> new AnswerEntry(
                (String) args[0],
                (String) args[1],
                (Double) args[2],
                (String) args[3],
                (Double) args[4],
                (Double) args[5],
                (List<ProbabilityEntry>) args[6]
            )
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareString(constructorArg(), new ParseField("name"));
            PARSER.declareDouble(optionalConstructorArg(), new ParseField("probability"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("choice"));
            PARSER.declareDouble(optionalConstructorArg(), new ParseField("score"));
            PARSER.declareDouble(optionalConstructorArg(), new ParseField("confidence"));
            PARSER.declareObjectArray(optionalConstructorArg(), ProbabilityEntry.PARSER::apply, new ParseField("probabilities"));
        }

        DecisionResults.Answer toAnswer() {
            return switch (type) {
                case PREDICATE_TYPE -> new DecisionResults.PredicateAnswer(requireField("probability", probability));
                case CHOICE_TYPE -> new DecisionResults.ChoiceAnswer(
                    requireField("choice", choice),
                    toChoiceProbabilities(),
                    requireField("confidence", confidence)
                );
                case SCORE_TYPE -> new DecisionResults.ScoreAnswer(
                    requireField("score", score),
                    toLevelProbabilities(),
                    requireField("confidence", confidence)
                );
                default -> throw new IllegalArgumentException(
                    Strings.format("Unknown type [%s] for the answer to question [%s]", type, name)
                );
            };
        }

        private Map<String, Double> toChoiceProbabilities() {
            var result = new LinkedHashMap<String, Double>();
            for (var entry : requireField("probabilities", probabilities)) {
                if (entry.value() instanceof String choice) {
                    result.put(choice, entry.probability());
                } else {
                    throw invalidProbabilityValue("a string", entry);
                }
            }
            return result;
        }

        /**
         * Keys the probabilities by level label, ordered by the zero-based level position given by {@code value}.
         */
        private Map<String, Double> toLevelProbabilities() {
            var levels = new TreeMap<Integer, ProbabilityEntry>();
            for (var entry : requireField("probabilities", probabilities)) {
                if (entry.value() instanceof Integer position && position >= 0) {
                    if (levels.put(position, entry) != null) {
                        throw new IllegalArgumentException(
                            Strings.format("Duplicate probability [value] [%d] for the [%s] answer to question [%s]", position, type, name)
                        );
                    }
                } else {
                    throw invalidProbabilityValue("a non-negative integer", entry);
                }
            }
            var result = new LinkedHashMap<String, Double>();
            for (var entry : levels.values()) {
                result.put(requireField("probabilities.label", entry.label()), entry.probability());
            }
            return result;
        }

        private IllegalArgumentException invalidProbabilityValue(String expected, ProbabilityEntry entry) {
            return new IllegalArgumentException(
                Strings.format(
                    "Field [probabilities.value] must be %s for the [%s] answer to question [%s], but found [%s]",
                    expected,
                    type,
                    name,
                    entry.value()
                )
            );
        }

        private <T> T requireField(String field, @Nullable T value) {
            if (value == null) {
                throw new IllegalArgumentException(
                    Strings.format("Required field [%s] is missing for the [%s] answer to question [%s]", field, type, name)
                );
            }
            return value;
        }
    }

    /**
     * The probability of a single option. {@code value} is the choice (a string) for choice answers and the zero-based level position
     * (an integer) for score answers, which additionally carry the level {@code label}.
     */
    record ProbabilityEntry(@Nullable Object value, @Nullable String label, double probability) {

        static final ConstructingObjectParser<ProbabilityEntry, Void> PARSER = new ConstructingObjectParser<>(
            ProbabilityEntry.class.getSimpleName(),
            true,
            args -> new ProbabilityEntry(args[0], (String) args[1], (Double) args[2])
        );

        static {
            // The value is a string for choice answers and an integer for score answers
            PARSER.declareField(optionalConstructorArg(), (p, c) -> parseValue(p), new ParseField("value"), ObjectParser.ValueType.VALUE);
            PARSER.declareString(optionalConstructorArg(), new ParseField("label"));
            PARSER.declareDouble(constructorArg(), new ParseField("probability"));
        }
    }

    /**
     * Keeps strings as strings and whole numbers as integers, anything else is rejected when the answer is built.
     */
    private static Object parseValue(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return parser.numberType() == XContentParser.NumberType.INT ? (Object) parser.intValue() : (Object) parser.doubleValue();
        }
        return parser.text();
    }

    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var decisionResponse = DecisionResponse.PARSER.apply(jsonParser, null);

            var answers = new LinkedHashMap<String, DecisionResults.Answer>();
            for (var answer : decisionResponse.answers()) {
                answers.put(answer.name(), answer.toAnswer());
            }

            return new DecisionResults(answers);
        }
    }

    private ElasticInferenceServiceDecisionResponseEntity() {}
}
