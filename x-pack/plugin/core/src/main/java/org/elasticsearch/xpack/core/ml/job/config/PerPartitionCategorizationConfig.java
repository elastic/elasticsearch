/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class PerPartitionCategorizationConfig implements ToXContentObject, Writeable {

    public static final ParseField TYPE_FIELD = new ParseField("per_partition_categorization");
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField STOP_ON_WARN = new ParseField("stop_on_warn");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<PerPartitionCategorizationConfig, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<PerPartitionCategorizationConfig, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<PerPartitionCategorizationConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<PerPartitionCategorizationConfig, Void> parser =
            new ConstructingObjectParser<>(TYPE_FIELD.getPreferredName(), ignoreUnknownFields,
                a -> new PerPartitionCategorizationConfig((boolean) a[0], (Boolean) a[1]));

        parser.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), STOP_ON_WARN);

        return parser;
    }

    private final boolean enabled;
    private final boolean stopOnWarn;

    public PerPartitionCategorizationConfig() {
        this(false, null);
    }

    public PerPartitionCategorizationConfig(boolean enabled, Boolean stopOnWarn) {
        this.enabled = enabled;
        this.stopOnWarn = (stopOnWarn == null) ? false : stopOnWarn;
        if (this.enabled == false && this.stopOnWarn) {
            throw ExceptionsHelper.badRequestException(STOP_ON_WARN.getPreferredName() + " cannot be true in "
                + TYPE_FIELD.getPreferredName() + " when " + ENABLED_FIELD.getPreferredName() + " is false");
        }
    }

    public PerPartitionCategorizationConfig(StreamInput in) throws IOException {
        enabled = in.readBoolean();
        stopOnWarn = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeBoolean(stopOnWarn);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        if (enabled) {
            builder.field(STOP_ON_WARN.getPreferredName(), stopOnWarn);
        }
        builder.endObject();
        return builder;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isStopOnWarn() {
        return stopOnWarn;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof PerPartitionCategorizationConfig == false) {
            return false;
        }

        PerPartitionCategorizationConfig that = (PerPartitionCategorizationConfig) other;
        return this.enabled == that.enabled && this.stopOnWarn == that.stopOnWarn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, stopOnWarn);
    }
}
