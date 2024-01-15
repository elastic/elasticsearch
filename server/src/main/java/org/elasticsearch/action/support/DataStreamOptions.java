/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Controls which indices associated with a data stream to include. Currently, it handles backing indices and the failure
 * store indices.
 */
public record DataStreamOptions(FailureStore failureStore) implements ToXContentFragment, Writeable {

    private static final ParseField FAILURE_STORE_FIELD = new ParseField("failure_store");

    public enum FailureStore {
        INCLUDE,
        EXCLUDE,
        ONLY;

        public static FailureStore parseParameter(String value, FailureStore defaultState) {
            if (value == null) {
                return defaultState;
            }

            return switch (value) {
                case "include" -> INCLUDE;
                case "exclude" -> EXCLUDE;
                case "only" -> ONLY;
                default -> throw new IllegalArgumentException("Value [" + value + "] is not a valid failure store option");
            };
        }

        String toXContentValue() {
            return toString().toLowerCase(Locale.ROOT);
        }

        public static boolean includeBackingIndices(@Nullable FailureStore failureStore) {
            return failureStore == null || failureStore.equals(ONLY) == false;
        }

        public static boolean includeFailureIndices(@Nullable FailureStore failureStore) {
            return failureStore != null && failureStore.equals(EXCLUDE) == false;
        }
    }

    public static final DataStreamOptions EXCLUDE_FAILURE_STORE = new DataStreamOptions(FailureStore.EXCLUDE);

    public static final DataStreamOptions INCLUDE_FAILURE_STORE = new DataStreamOptions(FailureStore.INCLUDE);

    public static final DataStreamOptions ONLY_FAILURE_STORE = new DataStreamOptions(FailureStore.ONLY);

    public boolean includeBackingIndices() {
        return FailureStore.includeBackingIndices(failureStore);
    }

    public boolean includeFailureIndices() {
        return FailureStore.includeFailureIndices(failureStore);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(failureStore);
    }

    public static DataStreamOptions read(StreamInput in) throws IOException {
        return new DataStreamOptions(in.readEnum(FailureStore.class));
    }

    public static DataStreamOptions fromRequest(RestRequest request, DataStreamOptions defaultSettings) {
        return fromParameters(request.param("failure_store"), defaultSettings);
    }

    public static DataStreamOptions fromMap(Map<String, Object> map, DataStreamOptions defaultSettings) {
        return fromParameters(
            (String) (map.containsKey("failure_store") ? map.get("failure_store") : map.get("failureStore")),
            defaultSettings
        );
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isDataStreamOption(String name) {
        return "failure_store".equals(name);
    }

    public static DataStreamOptions fromParameters(String failureStoreString, DataStreamOptions defaultSettings) {
        if (failureStoreString == null) {
            return defaultSettings;
        }

        return new DataStreamOptions(FailureStore.parseParameter(failureStoreString, defaultSettings.failureStore));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (failureStore != null) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore.toXContentValue());
        }
        return builder;
    }

    @Override
    public String toString() {
        return "DataStreamOptions{" + "failureStore=" + failureStore + '}';
    }
}
