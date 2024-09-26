/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Holds the data stream failure store metadata that enable or disable the failure store of a data stream. Currently, it
 * supports the following configurations only explicitly enabling or disabling the failure store
 */
public record DataStreamFailureStore(Boolean enabled) implements SimpleDiffable<DataStreamFailureStore>, ToXContentObject {

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    public static final ConstructingObjectParser<DataStreamFailureStore, Void> PARSER = new ConstructingObjectParser<>(
        "failure_store",
        false,
        (args, unused) -> new DataStreamFailureStore((Boolean) args[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    /**
     *  @param enabled, true when the failure is enabled, false when it's disabled, null when it depends on other configuration. Currently,
     *                  null value is not supported because there are no other arguments
     * @throws IllegalArgumentException when all the constructor arguments are null
     */
    public DataStreamFailureStore {
        if (enabled == null) {
            throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
        }
    }

    public DataStreamFailureStore(StreamInput in) throws IOException {
        this(in.readOptionalBoolean());
    }

    public static Diff<DataStreamFailureStore> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamFailureStore::new, in);
    }

    /**
     * @return iff the user has explicitly enabled the failure store
     */
    public boolean isExplicitlyEnabled() {
        return enabled != null && enabled;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(enabled);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (enabled != null) {
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamFailureStore fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
