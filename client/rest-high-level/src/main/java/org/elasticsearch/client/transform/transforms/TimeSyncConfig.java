/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSyncConfig implements SyncConfig {

    public static final String NAME = "time";

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField DELAY = new ParseField("delay");

    private final String field;
    private final TimeValue delay;

    private static final ConstructingObjectParser<TimeSyncConfig, Void> PARSER = new ConstructingObjectParser<>("time_sync_config", true,
            args -> new TimeSyncConfig((String) args[0], args[1] != null ? (TimeValue) args[1] : TimeValue.ZERO));

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DELAY.getPreferredName()), DELAY,
                ObjectParser.ValueType.STRING_OR_NULL);
    }

    public static TimeSyncConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    // Deprecated, the public modifier will be removed in 8.0: use the builder instead
    @Deprecated
    public TimeSyncConfig(String field, TimeValue delay) {
        this.field = field;
        this.delay = delay;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        if (delay.duration() > 0) {
            builder.field(DELAY.getPreferredName(), delay.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public TimeValue getDelay() {
        return delay;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TimeSyncConfig that = (TimeSyncConfig) other;

        return Objects.equals(this.field, that.field)
                && Objects.equals(this.delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, delay);
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
        private TimeValue delay = TimeValue.ZERO;

        /**
         * The date field that is used to identify new documents in the source.
         * @param field The field name of the timestamp field used for synchronizing
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * The time delay between the current time and the latest input data time.
         * The default value is 60s.
         * @param delay the delay to use when checking for changes
         * @return The {@link Builder} with delay set.
         */
        public Builder setDelay(TimeValue delay) {
            this.delay = delay;
            return this;
        }

        public TimeSyncConfig build() {
            return new TimeSyncConfig(field, delay);
        }
    }
}
