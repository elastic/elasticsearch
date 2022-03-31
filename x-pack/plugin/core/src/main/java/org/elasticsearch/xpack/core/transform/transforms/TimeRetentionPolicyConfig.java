/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class TimeRetentionPolicyConfig implements RetentionPolicyConfig {

    private static final String NAME = "transform_retention_policy_time";
    private static long MIN_AGE_SECONDS = 60;

    private final String field;
    private final TimeValue maxAge;

    private static final ConstructingObjectParser<TimeRetentionPolicyConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TimeRetentionPolicyConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TimeRetentionPolicyConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TimeRetentionPolicyConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            String field = (String) args[0];
            TimeValue maxAge = (TimeValue) args[1];

            return new TimeRetentionPolicyConfig(field, maxAge);
        });
        parser.declareString(constructorArg(), TransformField.FIELD);
        parser.declareField(
            constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), TransformField.MAX_AGE.getPreferredName()),
            TransformField.MAX_AGE,
            ObjectParser.ValueType.STRING
        );
        return parser;
    }

    public TimeRetentionPolicyConfig(final String field, final TimeValue maxAge) {
        this.field = ExceptionsHelper.requireNonNull(field, TransformField.FIELD.getPreferredName());
        this.maxAge = ExceptionsHelper.requireNonNull(maxAge, TransformField.MAX_AGE.getPreferredName());
    }

    public TimeRetentionPolicyConfig(StreamInput in) throws IOException {
        this.field = in.readString();
        this.maxAge = in.readTimeValue();
    }

    public String getField() {
        return field;
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }

    @Override
    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (maxAge.getSeconds() < MIN_AGE_SECONDS) {
            validationException = addValidationError(
                "retention_policy.time.max_age must be greater than " + MIN_AGE_SECONDS + "s, found [" + maxAge + "]",
                validationException
            );
        }

        if (maxAge.compareTo(TimeValue.MAX_VALUE) > 0) {
            validationException = addValidationError(
                "retention_policy.time.max_age must not be greater than [" + TimeValue.MAX_VALUE + "]",
                validationException
            );
        }

        return validationException;
    }

    @Override
    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {}

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeTimeValue(maxAge);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.FIELD.getPreferredName(), field);
        builder.field(TransformField.MAX_AGE.getPreferredName(), maxAge.getStringRep());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TimeRetentionPolicyConfig that = (TimeRetentionPolicyConfig) other;

        return Objects.equals(this.field, that.field) && Objects.equals(this.maxAge, that.maxAge);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, maxAge);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static TimeRetentionPolicyConfig parse(final XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    public static TimeRetentionPolicyConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    @Override
    public String getWriteableName() {
        return TransformField.TIME.getPreferredName();
    }
}
