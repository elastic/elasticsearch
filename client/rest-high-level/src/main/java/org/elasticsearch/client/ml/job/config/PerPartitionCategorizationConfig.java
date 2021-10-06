/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PerPartitionCategorizationConfig implements ToXContentObject {

    public static final ParseField TYPE_FIELD = new ParseField("per_partition_categorization");
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField STOP_ON_WARN = new ParseField("stop_on_warn");

    public static final ConstructingObjectParser<PerPartitionCategorizationConfig, Void> PARSER =
        new ConstructingObjectParser<>(TYPE_FIELD.getPreferredName(), true,
            a -> new PerPartitionCategorizationConfig((boolean) a[0], (Boolean) a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), STOP_ON_WARN);
    }

    private final boolean enabled;
    private final boolean stopOnWarn;

    public PerPartitionCategorizationConfig() {
        this(false, null);
    }

    public PerPartitionCategorizationConfig(boolean enabled, Boolean stopOnWarn) {
        this.enabled = enabled;
        this.stopOnWarn = (stopOnWarn == null) ? false : stopOnWarn;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
