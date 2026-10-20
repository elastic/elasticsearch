/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Writes a decision result in the following json format, where each answer is keyed by the name of the question it answers:
 * <pre>
 * {
 *   "decision": {
 *     "answers": {
 *       "is_complaint": {"type": "predicate", "probability": 0.94},
 *       "team": {
 *         "type": "choice",
 *         "choice": "billing",
 *         "probabilities": [
 *           {"value": "billing", "probability": 0.91},
 *           {"value": "shipping", "probability": 0.04},
 *           {"value": "other", "probability": 0.05}
 *         ],
 *         "confidence": 0.89
 *       },
 *       "anger": {
 *         "type": "score",
 *         "score": 1.4,
 *         "probabilities": [
 *           {"value": 0, "label": "calm", "probability": 0.1},
 *           {"value": 1, "label": "annoyed", "probability": 0.4},
 *           {"value": 2, "label": "furious", "probability": 0.5}
 *         ],
 *         "confidence": 0.62
 *       }
 *     }
 *   }
 * }
 * </pre>
 * The {@code type} of an answer matches the {@link DecisionQuestion.Type} of the question it answers. Like in the OpenAI Decisions API,
 * the {@code value} of a choice probability is the choice, and the {@code value} of a score probability is the zero-based position of
 * the level, which is also its position in the array.
 *
 * @param answers The answers keyed by question name. The iteration order is preserved.
 */
public record DecisionResults(Map<String, Answer> answers) implements InferenceServiceResults {

    public static final String NAME = "decision_service_results";
    public static final String DECISION = "decision";
    public static final String ANSWERS = "answers";

    /**
     * A parsed entry of the {@code probabilities} array. The {@code value} is a string for choice answers and an integer for score
     * answers, which additionally have a {@code label}.
     */
    private record ProbabilityEntry(Object value, @Nullable String label, double probability) {}

    private static final ConstructingObjectParser<ProbabilityEntry, Void> PROBABILITY_ENTRY_PARSER = createProbabilityEntryParser();

    private static ConstructingObjectParser<ProbabilityEntry, Void> createProbabilityEntryParser() {
        var parser = new ConstructingObjectParser<ProbabilityEntry, Void>(
            "decision_probability",
            true,
            a -> new ProbabilityEntry(a[0], (String) a[1], (double) a[2])
        );
        parser.declareField(constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.numberType() == XContentParser.NumberType.INT ? (Object) p.intValue() : (Object) p.doubleValue();
            }
            return p.text();
        }, new ParseField(Answer.VALUE), ObjectParser.ValueType.VALUE);
        parser.declareString(optionalConstructorArg(), new ParseField(Answer.LABEL));
        parser.declareDouble(constructorArg(), new ParseField(Answer.PROBABILITY));
        return parser;
    }

    public static ConstructingObjectParser<DecisionResults, Void> createParser(boolean ignoreUnknownFields) {
        var answersParser = new ConstructingObjectParser<DecisionResults, Void>(
            "decision_answers",
            ignoreUnknownFields,
            a -> (DecisionResults) a[0]
        );
        var answerParser = Answer.createParser(ignoreUnknownFields);
        answersParser.declareObject(constructorArg(), (p, c) -> parseAnswers(p, answerParser), new ParseField(ANSWERS));

        var parser = new ConstructingObjectParser<DecisionResults, Void>(
            "decision_results",
            ignoreUnknownFields,
            a -> (DecisionResults) a[0]
        );
        parser.declareObject(constructorArg(), answersParser, new ParseField(DECISION));
        return parser;
    }

    private static DecisionResults parseAnswers(XContentParser parser, ConstructingObjectParser<Answer, Void> answerParser)
        throws IOException {
        var answers = new LinkedHashMap<String, Answer>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            var name = parser.currentName();
            parser.nextToken();
            answers.put(name, answerParser.parse(parser, null));
        }
        return new DecisionResults(answers);
    }

    public DecisionResults {
        answers = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(answers)));
    }

    public DecisionResults(StreamInput in) throws IOException {
        this(in.readOrderedMap(StreamInput::readString, Answer::readFrom));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(DECISION),
            ChunkedToXContentHelper.xContentObjectFields(ANSWERS, answers),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(answers, StreamOutput::writeString, Answer::writeWithType);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Map<String, Object> asMap() {
        var answersMap = new LinkedHashMap<String, Object>();
        answers.forEach((name, answer) -> answersMap.put(name, answer.asMap()));
        return Map.of(DECISION, Map.of(ANSWERS, answersMap));
    }

    /**
     * The answer to a single question.
     */
    public sealed interface Answer extends Writeable, ToXContentObject permits PredicateAnswer, ChoiceAnswer, ScoreAnswer {

        String TYPE = "type";
        String PROBABILITY = "probability";
        String CHOICE = "choice";
        String SCORE = "score";
        String PROBABILITIES = "probabilities";
        String CONFIDENCE = "confidence";
        String VALUE = "value";
        String LABEL = "label";

        DecisionQuestion.Type type();

        Map<String, Object> asMap();

        @SuppressWarnings("unchecked")
        private static ConstructingObjectParser<Answer, Void> createParser(boolean ignoreUnknownFields) {
            var parser = new ConstructingObjectParser<Answer, Void>(
                "decision_answer",
                ignoreUnknownFields,
                a -> of(
                    (DecisionQuestion.Type) a[0],
                    (Double) a[1],
                    (String) a[2],
                    (Double) a[3],
                    (List<ProbabilityEntry>) a[4],
                    (Double) a[5]
                )
            );
            parser.declareString(constructorArg(), DecisionQuestion.Type::fromString, new ParseField(TYPE));
            parser.declareDouble(optionalConstructorArg(), new ParseField(PROBABILITY));
            parser.declareString(optionalConstructorArg(), new ParseField(CHOICE));
            parser.declareDouble(optionalConstructorArg(), new ParseField(SCORE));
            parser.declareObjectArray(
                optionalConstructorArg(),
                (p, c) -> PROBABILITY_ENTRY_PARSER.apply(p, c),
                new ParseField(PROBABILITIES)
            );
            parser.declareDouble(optionalConstructorArg(), new ParseField(CONFIDENCE));
            return parser;
        }

        private static Answer of(
            DecisionQuestion.Type type,
            @Nullable Double probability,
            @Nullable String choice,
            @Nullable Double score,
            @Nullable List<ProbabilityEntry> probabilities,
            @Nullable Double confidence
        ) {
            return switch (type) {
                case PREDICATE -> new PredicateAnswer(requireField(type, PROBABILITY, probability));
                case CHOICE -> new ChoiceAnswer(
                    requireField(type, CHOICE, choice),
                    toChoiceProbabilities(requireField(type, PROBABILITIES, probabilities)),
                    requireField(type, CONFIDENCE, confidence)
                );
                case SCORE -> new ScoreAnswer(
                    requireField(type, SCORE, score),
                    toLevelProbabilities(requireField(type, PROBABILITIES, probabilities)),
                    requireField(type, CONFIDENCE, confidence)
                );
            };
        }

        private static <T> T requireField(DecisionQuestion.Type type, String field, @Nullable T value) {
            if (value == null) {
                throw new IllegalArgumentException(Strings.format("Field [%s] is required for answers of type [%s]", field, type));
            }
            return value;
        }

        private static Map<String, Double> toChoiceProbabilities(List<ProbabilityEntry> entries) {
            var probabilities = new LinkedHashMap<String, Double>();
            for (var entry : entries) {
                if (entry.value() instanceof String choice) {
                    probabilities.put(choice, entry.probability());
                } else {
                    throw new IllegalArgumentException(
                        Strings.format("Field [%s.%s] must be a string for answers of type [choice]", PROBABILITIES, VALUE)
                    );
                }
            }
            return probabilities;
        }

        /**
         * Keys the score probabilities by level label, ordered by the zero-based level position given by {@code value}.
         */
        private static Map<String, Double> toLevelProbabilities(List<ProbabilityEntry> entries) {
            var levels = new TreeMap<Integer, ProbabilityEntry>();
            for (var entry : entries) {
                if (entry.value() instanceof Integer position && position >= 0) {
                    if (levels.put(position, entry) != null) {
                        throw new IllegalArgumentException(
                            Strings.format("Field [%s] contains the value [%d] twice", PROBABILITIES, position)
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        Strings.format("Field [%s.%s] must be a non-negative integer for answers of type [score]", PROBABILITIES, VALUE)
                    );
                }
            }
            var probabilities = new LinkedHashMap<String, Double>();
            for (var entry : levels.values()) {
                probabilities.put(
                    requireField(DecisionQuestion.Type.SCORE, PROBABILITIES + "." + LABEL, entry.label()),
                    entry.probability()
                );
            }
            return probabilities;
        }

        private static Answer readFrom(StreamInput in) throws IOException {
            var type = DecisionQuestion.Type.fromStream(in);
            return switch (type) {
                case PREDICATE -> new PredicateAnswer(in);
                case CHOICE -> new ChoiceAnswer(in);
                case SCORE -> new ScoreAnswer(in);
            };
        }

        /**
         * Writes the {@link DecisionQuestion.Type} first, so {@link #readFrom(StreamInput)} knows which answer to read.
         */
        private static void writeWithType(StreamOutput out, Answer answer) throws IOException {
            answer.type().writeTo(out);
            answer.writeTo(out);
        }
    }

    /**
     * The answer to a {@link DecisionQuestion.Predicate} question.
     *
     * @param probability The probability that the statement of the question is true
     */
    public record PredicateAnswer(double probability) implements Answer {

        PredicateAnswer(StreamInput in) throws IOException {
            this(in.readDouble());
        }

        @Override
        public DecisionQuestion.Type type() {
            return DecisionQuestion.Type.PREDICATE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(probability);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE, type().toString());
            builder.field(PROBABILITY, probability);
            builder.endObject();
            return builder;
        }

        @Override
        public Map<String, Object> asMap() {
            return Map.of(TYPE, type().toString(), PROBABILITY, probability);
        }
    }

    /**
     * The answer to a {@link DecisionQuestion.Choice} question.
     *
     * @param choice        The selected choice
     * @param probabilities The probability of each choice. The iteration order is preserved.
     * @param confidence    The confidence in the selected choice
     */
    public record ChoiceAnswer(String choice, Map<String, Double> probabilities, double confidence) implements Answer {

        public ChoiceAnswer {
            Objects.requireNonNull(choice);
            probabilities = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(probabilities)));
        }

        ChoiceAnswer(StreamInput in) throws IOException {
            this(in.readString(), in.readOrderedMap(StreamInput::readString, StreamInput::readDouble), in.readDouble());
        }

        @Override
        public DecisionQuestion.Type type() {
            return DecisionQuestion.Type.CHOICE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(choice);
            out.writeMap(probabilities, StreamOutput::writeString, StreamOutput::writeDouble);
            out.writeDouble(confidence);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE, type().toString());
            builder.field(CHOICE, choice);
            builder.startArray(PROBABILITIES);
            for (var option : probabilities.entrySet()) {
                builder.startObject();
                builder.field(VALUE, option.getKey());
                builder.field(PROBABILITY, option.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.field(CONFIDENCE, confidence);
            builder.endObject();
            return builder;
        }

        @Override
        public Map<String, Object> asMap() {
            var options = new ArrayList<Map<String, Object>>();
            for (var option : probabilities.entrySet()) {
                options.add(Map.of(VALUE, option.getKey(), PROBABILITY, option.getValue()));
            }
            return Map.of(TYPE, type().toString(), CHOICE, choice, PROBABILITIES, options, CONFIDENCE, confidence);
        }
    }

    /**
     * The answer to a {@link DecisionQuestion.Score} question.
     *
     * @param score         The probability-weighted average of the zero-based level positions
     * @param probabilities The probability of each level, ordered from lowest to highest level
     * @param confidence    The confidence in the score
     */
    public record ScoreAnswer(double score, Map<String, Double> probabilities, double confidence) implements Answer {

        public ScoreAnswer {
            probabilities = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(probabilities)));
        }

        ScoreAnswer(StreamInput in) throws IOException {
            this(in.readDouble(), in.readOrderedMap(StreamInput::readString, StreamInput::readDouble), in.readDouble());
        }

        @Override
        public DecisionQuestion.Type type() {
            return DecisionQuestion.Type.SCORE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(score);
            out.writeMap(probabilities, StreamOutput::writeString, StreamOutput::writeDouble);
            out.writeDouble(confidence);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE, type().toString());
            builder.field(SCORE, score);
            builder.startArray(PROBABILITIES);
            int value = 0;
            for (var level : probabilities.entrySet()) {
                builder.startObject();
                builder.field(VALUE, value++);
                builder.field(LABEL, level.getKey());
                builder.field(PROBABILITY, level.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.field(CONFIDENCE, confidence);
            builder.endObject();
            return builder;
        }

        @Override
        public Map<String, Object> asMap() {
            var levels = new ArrayList<Map<String, Object>>();
            for (var level : probabilities.entrySet()) {
                levels.add(Map.of(VALUE, levels.size(), LABEL, level.getKey(), PROBABILITY, level.getValue()));
            }
            return Map.of(TYPE, type().toString(), SCORE, score, PROBABILITIES, levels, CONFIDENCE, confidence);
        }
    }
}
