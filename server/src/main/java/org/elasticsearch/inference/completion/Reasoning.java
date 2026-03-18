/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ENABLED_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EXCLUDE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.getUnrecognizedTypeException;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents the reasoning configuration for a completion request.
 * It encapsulates various parameters that control the reasoning process.
 * @param effort The {@link ReasoningEffort} level to apply. This is an optional parameter.
 * @param summary The {@link ReasoningSummary} level to provide. This is an optional parameter.
 * @param exclude Whether to exclude reasoning from the response. This is an optional parameter.
 * @param enabled Whether to enable reasoning. This is an optional parameter.
 * @see ReasoningEffort
 * @see ReasoningSummary
 */
public record Reasoning(
    @Nullable ReasoningEffort effort,
    @Nullable ReasoningSummary summary,
    @Nullable Boolean exclude,
    @Nullable Boolean enabled
) implements ToXContentObject, NamedWriteable {

    public static final String NAME = "reasoning";

    public static final ConstructingObjectParser<Reasoning, Void> PARSER = new ConstructingObjectParser<>(
        Reasoning.class.getSimpleName(),
        args -> {
            final var effort = args[0] == null ? null : ReasoningEffort.fromString((String) args[0]);
            final var summary = args[1] == null ? null : ReasoningSummary.fromString((String) args[1]);
            final var exclude = (Boolean) args[2];
            final var enabled = (Boolean) args[3];

            validateFields(effort, enabled);

            return new Reasoning(effort, summary, exclude, enabled);
        }
    );

    static {
        PARSER.declareRequiredFieldSet(EFFORT_FIELD, ENABLED_FIELD);

        PARSER.declareString(optionalConstructorArg(), new ParseField(EFFORT_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(SUMMARY_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(EXCLUDE_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(ENABLED_FIELD));
    }

    /**
     * Method to validate the reasoning configuration.
     * It ensures that:
     * <ul>
     *     <li>If [effort] is null and [enabled] is false, an exception is thrown.</li>
     * </ul>
     * @param effort The reasoning effort level to validate.
     * @param enabled `enabled` field to validate.
     */
    private static void validateFields(@Nullable ReasoningEffort effort, @Nullable Boolean enabled) {
        if (Boolean.FALSE.equals(enabled) && effort == null) {
            throw new ElasticsearchStatusException("When [enabled] is false, [effort] must be specified.", RestStatus.BAD_REQUEST);
        }
    }

    public Reasoning(StreamInput in) throws IOException {
        this(
            in.readOptionalEnum(ReasoningEffort.class),
            in.readOptionalEnum(ReasoningSummary.class),
            in.readOptionalBoolean(),
            in.readOptionalBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(effort);
        out.writeOptionalEnum(summary);
        out.writeOptionalBoolean(exclude);
        out.writeOptionalBoolean(enabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (effort != null) {
            builder.field(EFFORT_FIELD, effort);
        }
        if (summary != null) {
            builder.field(SUMMARY_FIELD, summary);
        }
        if (exclude != null) {
            builder.field(EXCLUDE_FIELD, exclude);
        }
        if (enabled != null) {
            builder.field(ENABLED_FIELD, enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Enum representing the reasoning effort levels.
     */
    public enum ReasoningEffort {
        XHIGH,
        HIGH,
        MEDIUM,
        LOW,
        MINIMAL,
        NONE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ReasoningEffort fromString(String name) {
            try {
                return valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw getUnrecognizedTypeException(name, EFFORT_FIELD, ReasoningEffort.class);
            }
        }
    }

    /**
     * Enum representing the reasoning summary levels.
     */
    public enum ReasoningSummary {
        AUTO,
        CONCISE,
        DETAILED;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ReasoningSummary fromString(String name) {
            try {
                return valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw getUnrecognizedTypeException(name, SUMMARY_FIELD, ReasoningSummary.class);
            }
        }
    }
}
