/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class TimeRetentionPolicyConfig implements RetentionPolicyConfig {

    public static final String NAME = "time";

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField MAX_AGE = new ParseField("max_age");

    private final String field;
    private final TimeValue maxAge;

    private static final ConstructingObjectParser<TimeRetentionPolicyConfig, Void> PARSER = new ConstructingObjectParser<>(
        "time_retention_policy_config",
        true,
        args -> new TimeRetentionPolicyConfig((String) args[0], args[1] != null ? (TimeValue) args[1] : TimeValue.ZERO)
    );

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_AGE.getPreferredName()),
            MAX_AGE,
            ObjectParser.ValueType.STRING
        );
    }

    public static TimeRetentionPolicyConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    TimeRetentionPolicyConfig(String field, TimeValue maxAge) {
        this.field = field;
        this.maxAge = maxAge;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(MAX_AGE.getPreferredName(), maxAge.getStringRep());
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public TimeValue getMaxAge() {
        return maxAge;
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
    public String getName() {
        return NAME;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String field;
        private TimeValue maxAge;

        /**
         * The time field used to calculate the age of a document.
         * @param field The field name to be used to execute the retention policy
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * The max age, all documents that are older will be deleted.
         * @param maxAge The maximum age of a document
         * @return The {@link Builder} with max age set.
         */
        public Builder setMaxAge(TimeValue maxAge) {
            this.maxAge = maxAge;
            return this;
        }

        public TimeRetentionPolicyConfig build() {
            return new TimeRetentionPolicyConfig(field, maxAge);
        }
    }
}
