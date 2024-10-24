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
import org.elasticsearch.xcontent.ObjectParser;
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
        (args, unused) -> new DataStreamFailureStore((DataStreamOptions.NullableFlag) args[0])
    );

    @Nullable
    private final DataStreamOptions.NullableFlag enabled;

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (p.isBooleanValue() == false) {
                return DataStreamOptions.NullableFlag.NULL_VALUE;
            } else {
                return p.booleanValue() ? DataStreamOptions.NullableFlag.TRUE : DataStreamOptions.NullableFlag.FALSE;
            }
        }, ENABLED_FIELD, ObjectParser.ValueType.BOOLEAN_OR_NULL);
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
    public DataStreamFailureStore(@Nullable Boolean enabled) {
        this(DataStreamOptions.NullableFlag.fromBoolean(enabled));
    }

    /**
     * @param enabled, the flag configuration, that can be true, false and implicit or explicit null. Currently,
     *                 null value is not supported because there are no other arguments
     * @throws IllegalArgumentException when all the constructor arguments are null
     */
    public DataStreamFailureStore(@Nullable DataStreamOptions.NullableFlag enabled) {
        if (DataStreamOptions.NullableFlag.isDefined(enabled) == false) {
            throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
        }
        this.enabled = enabled;
    }

    public static DataStreamFailureStore read(StreamInput in) throws IOException {
        var enabled = in.readOptionalWriteable(DataStreamOptions.NullableFlag::read);
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
    public DataStreamOptions.NullableFlag enabled() {
        return enabled;
    }

    /**
     * @return failure store configuration that replaces explicit null value with null.
     */
    @Nullable
    public static DataStreamFailureStore resolveExplicitNullValues(DataStreamFailureStore failureStore) {
        if (failureStore == null || failureStore.enabled == DataStreamOptions.NullableFlag.NULL_VALUE || failureStore.isNullified()) {
            return null;
        }
        return failureStore;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(enabled);
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
                builder.field(ENABLED_FIELD.getPreferredName());
                enabled.toXContent(builder, params);
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
