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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A single question answered by the {@link TaskType#DECISION} task type. Every question has a {@link Type} and {@code instructions}
 * describing what to decide. Depending on the type, a question carries additional fields:
 * <pre>
 * {"type": "predicate", "instructions": "Is the customer complaining?"}
 *
 * {
 *   "type": "choice",
 *   "instructions": "Which team should handle this?",
 *   "choices": {"billing": "Charges, refunds, invoices", "other": null}
 * }
 *
 * {
 *   "type": "score",
 *   "instructions": "How angry is the customer?",
 *   "levels": ["calm", "annoyed", {"label": "furious", "description": "Threatens to leave"}]
 * }</pre>
 * {@code choices} maps each option to an optional description, {@code levels} are ordered from lowest to highest. A level is either a
 * plain label or an object with a {@code label} and an optional {@code description}.
 */
public sealed interface DecisionQuestion extends Writeable, ToXContentObject permits DecisionQuestion.Predicate, DecisionQuestion.Choice,
    DecisionQuestion.Score {

    String TYPE_FIELD = "type";
    String INSTRUCTIONS_FIELD = "instructions";
    String CHOICES_FIELD = "choices";
    String LEVELS_FIELD = "levels";
    String LABEL_FIELD = "label";
    String DESCRIPTION_FIELD = "description";

    int MIN_CHOICES = 2;
    int MIN_LEVELS = 2;

    /**
     * The kind of a question, which determines the shape of both the question and its answer.
     * <p>
     * Questions are persisted with the inference endpoint and sent between nodes, so a node that does not know a type can neither parse
     * nor read it. Adding a new type therefore requires guarding its use, for example behind a node feature and a transport version,
     * until every node in the cluster is able to parse it.
     */
    enum Type implements Writeable {
        /**
         * The probability that a statement is true.
         */
        PREDICATE,
        /**
         * One option out of a fixed set of choices.
         */
        CHOICE,
        /**
         * The probability-weighted position on an ordered scale of levels.
         */
        SCORE;

        public static Type fromString(String name) {
            try {
                return valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    Strings.format("Unknown question [%s] value [%s]. Supported values are %s", TYPE_FIELD, name, Arrays.toString(values()))
                );
            }
        }

        public static Type fromStream(StreamInput in) throws IOException {
            return in.readEnum(Type.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    Type type();

    String instructions();

    /**
     * Creates a parser for a single question. With {@code ignoreUnknownFields} set to {@code false} (used for user requests) unknown
     * fields are rejected, with {@code true} (used for persisted endpoints) they are ignored, so questions stored by a newer version can
     * still be read.
     */
    @SuppressWarnings("unchecked")
    static ConstructingObjectParser<DecisionQuestion, Void> createParser(boolean ignoreUnknownFields) {
        var parser = new ConstructingObjectParser<DecisionQuestion, Void>(
            "decision_question",
            ignoreUnknownFields,
            args -> of((Type) args[0], (String) args[1], (Map<String, String>) args[2], (List<Level>) args[3])
        );
        parser.declareString(constructorArg(), Type::fromString, new ParseField(TYPE_FIELD));
        parser.declareString(constructorArg(), new ParseField(INSTRUCTIONS_FIELD));
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> parseChoices(p),
            new ParseField(CHOICES_FIELD),
            ObjectParser.ValueType.OBJECT
        );
        var levelParser = createLevelParser(ignoreUnknownFields);
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> parseLevels(p, levelParser),
            new ParseField(LEVELS_FIELD),
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        return parser;
    }

    /**
     * Creates the question of the given {@code type}, rejecting fields that do not belong to it.
     */
    private static DecisionQuestion of(
        Type type,
        String instructions,
        @Nullable Map<String, String> choices,
        @Nullable List<Level> levels
    ) {
        return switch (type) {
            case PREDICATE -> {
                rejectField(type, CHOICES_FIELD, choices);
                rejectField(type, LEVELS_FIELD, levels);
                yield new Predicate(instructions);
            }
            case CHOICE -> {
                rejectField(type, LEVELS_FIELD, levels);
                yield new Choice(instructions, requireField(type, CHOICES_FIELD, choices));
            }
            case SCORE -> {
                rejectField(type, CHOICES_FIELD, choices);
                yield new Score(instructions, requireField(type, LEVELS_FIELD, levels));
            }
        };
    }

    static DecisionQuestion readFrom(StreamInput in) throws IOException {
        var type = Type.fromStream(in);
        return switch (type) {
            case PREDICATE -> new Predicate(in);
            case CHOICE -> new Choice(in);
            case SCORE -> new Score(in);
        };
    }

    /**
     * Writes the {@link Type} first, so {@link #readFrom(StreamInput)} knows which question to read.
     */
    static void writeWithType(StreamOutput out, DecisionQuestion question) throws IOException {
        question.type().writeTo(out);
        question.writeTo(out);
    }

    /**
     * Parses the {@code choices} object. Unlike {@link XContentParser#mapStrings()}, this allows {@code null} descriptions.
     */
    private static Map<String, String> parseChoices(XContentParser parser) throws IOException {
        var choices = new LinkedHashMap<String, String>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            var choice = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                choices.put(choice, parser.text());
            } else if (token == XContentParser.Token.VALUE_NULL) {
                choices.put(choice, null);
            } else {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    Strings.format("The description of choice [%s] must be a string or null, but found [%s]", choice, token)
                );
            }
        }
        return choices;
    }

    private static ConstructingObjectParser<Level, Void> createLevelParser(boolean ignoreUnknownFields) {
        var parser = new ConstructingObjectParser<Level, Void>(
            "decision_question_level",
            ignoreUnknownFields,
            args -> new Level((String) args[0], (String) args[1])
        );
        parser.declareString(constructorArg(), new ParseField(LABEL_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(DESCRIPTION_FIELD));
        return parser;
    }

    /**
     * Parses the {@code levels} array, in which every level is either a plain label or an object with a label and a description.
     */
    private static List<Level> parseLevels(XContentParser parser, ConstructingObjectParser<Level, Void> levelParser) throws IOException {
        var levels = new ArrayList<Level>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                levels.add(new Level(parser.text()));
            } else if (token == XContentParser.Token.START_OBJECT) {
                levels.add(levelParser.parse(parser, null));
            } else {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    Strings.format("Each of the [%s] must be a string or an object, but found [%s]", LEVELS_FIELD, token)
                );
            }
        }
        return levels;
    }

    private static void rejectField(Type type, String field, @Nullable Object value) {
        if (value != null) {
            throw new IllegalArgumentException(Strings.format("Field [%s] is not supported for questions of type [%s]", field, type));
        }
    }

    private static <T> T requireField(Type type, String field, @Nullable T value) {
        if (value == null) {
            throw new IllegalArgumentException(Strings.format("Field [%s] is required for questions of type [%s]", field, type));
        }
        return value;
    }

    private static String validateInstructions(String instructions) {
        if (Strings.isNullOrBlank(instructions)) {
            throw new IllegalArgumentException(Strings.format("Field [%s] must be a non-empty string", INSTRUCTIONS_FIELD));
        }
        return instructions;
    }

    /**
     * A question answered with the probability that the statement in {@code instructions} is true.
     */
    record Predicate(String instructions) implements DecisionQuestion {

        public Predicate {
            validateInstructions(instructions);
        }

        Predicate(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public Type type() {
            return Type.PREDICATE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(instructions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, type().toString());
            builder.field(INSTRUCTIONS_FIELD, instructions);
            builder.endObject();
            return builder;
        }
    }

    /**
     * A question answered with one of the given {@code choices}.
     *
     * @param choices The options to choose from, mapped to an optional description. The iteration order is preserved.
     */
    record Choice(String instructions, Map<String, String> choices) implements DecisionQuestion {

        public Choice {
            validateInstructions(instructions);
            Objects.requireNonNull(choices);
            if (choices.size() < MIN_CHOICES) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Field [%s] must contain at least [%d] choices, but found [%d]",
                        CHOICES_FIELD,
                        MIN_CHOICES,
                        choices.size()
                    )
                );
            }
            for (var choice : choices.keySet()) {
                if (Strings.isNullOrBlank(choice)) {
                    throw new IllegalArgumentException(Strings.format("Field [%s] must not contain empty choices", CHOICES_FIELD));
                }
            }
            // A plain copy, as Map.copyOf() neither preserves the order nor allows null descriptions
            choices = Collections.unmodifiableMap(new LinkedHashMap<>(choices));
        }

        Choice(StreamInput in) throws IOException {
            this(in.readString(), in.readOrderedMap(StreamInput::readString, StreamInput::readOptionalString));
        }

        @Override
        public Type type() {
            return Type.CHOICE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(instructions);
            out.writeMap(choices, StreamOutput::writeString, StreamOutput::writeOptionalString);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, type().toString());
            builder.field(INSTRUCTIONS_FIELD, instructions);
            builder.field(CHOICES_FIELD, choices);
            builder.endObject();
            return builder;
        }
    }

    /**
     * A level of the scale of a {@link Score} question.
     *
     * @param label The name of the level, which identifies it in the answer
     * @param description An optional explanation of what the level stands for
     */
    record Level(String label, @Nullable String description) implements Writeable {

        public Level {
            if (Strings.isNullOrBlank(label)) {
                throw new IllegalArgumentException(Strings.format("Field [%s] must not contain empty levels", LEVELS_FIELD));
            }
        }

        public Level(String label) {
            this(label, null);
        }

        Level(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(label);
            out.writeOptionalString(description);
        }

        /**
         * Writes a level without description as a plain label, which keeps the output parseable and the common case compact.
         */
        void toXContent(XContentBuilder builder) throws IOException {
            if (description == null) {
                builder.value(label);
            } else {
                builder.startObject();
                builder.field(LABEL_FIELD, label);
                builder.field(DESCRIPTION_FIELD, description);
                builder.endObject();
            }
        }
    }

    /**
     * A question answered with a position on the scale given by {@code levels}.
     *
     * @param levels The levels of the scale, ordered from lowest to highest
     */
    record Score(String instructions, List<Level> levels) implements DecisionQuestion {

        public Score {
            validateInstructions(instructions);
            Objects.requireNonNull(levels);
            if (levels.size() < MIN_LEVELS) {
                throw new IllegalArgumentException(
                    Strings.format("Field [%s] must contain at least [%d] levels, but found [%d]", LEVELS_FIELD, MIN_LEVELS, levels.size())
                );
            }
            if (levels.stream().map(Level::label).distinct().count() != levels.size()) {
                throw new IllegalArgumentException(Strings.format("Field [%s] must not contain duplicate levels", LEVELS_FIELD));
            }
            levels = List.copyOf(levels);
        }

        Score(StreamInput in) throws IOException {
            this(in.readString(), in.readCollectionAsImmutableList(Level::new));
        }

        /**
         * The labels of the levels, ordered from lowest to highest.
         */
        public List<String> labels() {
            return levels.stream().map(Level::label).toList();
        }

        @Override
        public Type type() {
            return Type.SCORE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(instructions);
            out.writeCollection(levels);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD, type().toString());
            builder.field(INSTRUCTIONS_FIELD, instructions);
            builder.startArray(LEVELS_FIELD);
            for (var level : levels) {
                level.toXContent(builder);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }
}
