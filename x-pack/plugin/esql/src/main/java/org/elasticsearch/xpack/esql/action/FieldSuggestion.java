/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A completion candidate field: its resolved type, plus at most one of {@link #values} or
 * {@link #range}. Both are {@code null} when data nodes were not visited (or statistics were
 * suppressed, e.g. under DLS).
 */
public record FieldSuggestion(String type, @Nullable List<ValueSuggestion> values, @Nullable RangeSuggestion range)
    implements
        ToXContentObject {

    public static FieldSuggestion ofType(String type) {
        return new FieldSuggestion(type, null, null);
    }

    /** A single sampled value for a field, with its document frequency in {@code [0, 1]}. */
    public record ValueSuggestion(Object value, double docFreq) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("value", value);
            builder.field("doc_freq", docFreq);
            builder.endObject();
            return builder;
        }
    }

    /** The min/max range observed for a range-eligible field. */
    public record RangeSuggestion(Object min, Object max) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min", min);
            builder.field("max", max);
            builder.endObject();
            return builder;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        if (values != null) {
            builder.startArray("values");
            for (ValueSuggestion value : values) {
                value.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (range != null) {
            builder.field("range");
            range.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
