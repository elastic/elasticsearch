/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.ENABLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.EXCLUDE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.SUMMARY_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.getUnrecognizedTypeException;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents the reasoning configuration for a completion request.
 * It encapsulates various parameters that control the reasoning process.
 * @param effort The {@link ReasoningEffort} level to apply. This is an optional parameter.
 * @param maxTokens The maximum number of tokens to use for reasoning. This is an optional parameter.
 * @param summary The {@link ReasoningSummary} level to provide. This is an optional parameter.
 * @param exclude Whether to exclude reasoning from the response. This is an optional parameter.
 * @param enable Whether to enable reasoning. This is an optional parameter.
 * @see ReasoningEffort
 * @see ReasoningSummary
 */
public record Reasoning(
    @Nullable ReasoningEffort effort,
    @Nullable Long maxTokens,
    @Nullable ReasoningSummary summary,
    @Nullable Boolean exclude,
    @Nullable Boolean enable
) implements ToXContent, NamedWriteable {

    public static final String NAME = "reasoning";

    public static final ConstructingObjectParser<Reasoning, Void> PARSER = new ConstructingObjectParser<>(
        Reasoning.class.getSimpleName(),
        args -> new Reasoning(
            args[0] == null ? null : ReasoningEffort.fromString((String) args[0]),
            (Long) args[1],
            args[2] == null ? null : ReasoningSummary.fromString((String) args[2]),
            (Boolean) args[3],
            (Boolean) args[4]
        )
    );

    static {
        /*
         * The reasoning configuration requires at least one of [effort, max_tokens, enable] to be provided.
         * [effort] and [max_tokens] cannot be specified together as they represent different ways to configure reasoning.
         */
        PARSER.declareRequiredFieldSet(EFFORT_FIELD, MAX_TOKENS_FIELD, ENABLE_FIELD);
        PARSER.declareExclusiveFieldSet(EFFORT_FIELD, MAX_TOKENS_FIELD);

        PARSER.declareString(optionalConstructorArg(), new ParseField(EFFORT_FIELD));
        PARSER.declareLong(optionalConstructorArg(), new ParseField(MAX_TOKENS_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(SUMMARY_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(EXCLUDE_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(ENABLE_FIELD));
    }

    public Reasoning(
        @Nullable ReasoningEffort effort,
        @Nullable Long maxTokens,
        @Nullable ReasoningSummary summary,
        @Nullable Boolean exclude,
        @Nullable Boolean enable
    ) {
        this.effort = effort;
        this.maxTokens = maxTokens;
        this.summary = summary;
        this.exclude = exclude;
        this.enable = enable;
        validate();
    }

    /**
     * Method to validate the reasoning configuration.
     * It ensures that:
     * <ul>
     *     <li>Either [effort] is not null, [max_tokens] is not null, or [enable] is true.
     *     If none of these conditions are met, an exception is thrown.</li>
     *     <li>Both [effort] and [max_tokens] cannot be specified together. If both are non-null, an exception is thrown.</li>
     * </ul>
     */
    private void validate() {
        if ((effort == null && maxTokens == null) && Boolean.TRUE.equals(enable) == false) {
            throw new IllegalArgumentException("Either [effort] or [max_tokens] must not be null, or [enable] must be true.");
        }
        if (effort != null && maxTokens != null) {
            throw new IllegalArgumentException("[effort] and [max_tokens] cannot be specified together.");
        }
    }

    public Reasoning(StreamInput in) throws IOException {
        this(
            in.readOptionalEnum(ReasoningEffort.class),
            in.readOptionalVLong(),
            in.readOptionalEnum(ReasoningSummary.class),
            in.readOptionalBoolean(),
            in.readOptionalBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(effort);
        out.writeOptionalVLong(maxTokens);
        out.writeOptionalEnum(summary);
        out.writeOptionalBoolean(exclude);
        out.writeOptionalBoolean(enable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (effort != null) {
            builder.field(EFFORT_FIELD, effort);
        }
        if (maxTokens != null) {
            builder.field(MAX_TOKENS_FIELD, maxTokens);
        }
        if (summary != null) {
            builder.field(SUMMARY_FIELD, summary);
        }
        if (exclude != null) {
            builder.field(EXCLUDE_FIELD, exclude);
        }
        if (enable != null) {
            builder.field(ENABLE_FIELD, enable);
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
