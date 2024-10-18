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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds the data stream failure store metadata that enable or disable the failure store of a data stream. Currently, it
 * supports the following configurations only explicitly enabling or disabling the failure store
 */
public class DataStreamFailureStore implements SimpleDiffable<DataStreamFailureStore>, ToXContentObject {

    public static final DataStreamFailureStore NULL = new DataStreamFailureStore();
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    public static final ConstructingObjectParser<DataStreamFailureStore, Void> PARSER = new ConstructingObjectParser<>(
        "failure_store",
        false,
        (args, unused) -> new DataStreamFailureStore((Boolean) args[0])
    );

    @Nullable
    private final Boolean enabled;

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    /**
     * This constructor is only used to represent the <code>null</code> value in a template.
     */
    private DataStreamFailureStore() {
        this.enabled = null;
    }

    /**
     * @param enabled, true when the failure is enabled, false when it's disabled, null when it depends on other configuration. Currently,
     *                 null value is not supported because there are no other arguments
     * @throws IllegalArgumentException when all the constructor arguments are null
     */
    public DataStreamFailureStore(Boolean enabled) {
        if (enabled == null) {
            throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
        }
        this.enabled = enabled;
    }

    public static DataStreamFailureStore read(StreamInput in) throws IOException {
        var enabled = in.readOptionalBoolean();
        if (enabled == null) {
            return NULL;
        }
        return new DataStreamFailureStore(enabled);
    }

    public static Diff<DataStreamFailureStore> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamFailureStore::read, in);
    }

    /**
     * @return iff the value represents a user explicitly nullifying the failure store.
     */
    public boolean isNullified() {
        return equals(NULL);
    }

    /**
     * @return exposes the value of the enabled flag
     */
    @Nullable
    public Boolean enabled() {
        return enabled;
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
        if (isNullified()) {
            builder.nullValue();
        } else {
            builder.startObject();
            if (enabled != null) {
                builder.field(ENABLED_FIELD.getPreferredName(), enabled);
            }
            builder.endObject();
        }
        return builder;
    }

    public static DataStreamFailureStore fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataStreamFailureStore) obj;
        return Objects.equals(this.enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }
}
