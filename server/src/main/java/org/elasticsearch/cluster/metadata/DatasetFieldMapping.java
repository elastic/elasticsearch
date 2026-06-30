/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * One declared column inside a dataset's {@link DatasetMapping} {@code properties} block — the value
 * of a {@code "logical_name": { ... }} entry.
 *
 * <p>Carries the declared {@code type} and an optional {@code source} (the physical column name in the
 * file, when the logical name differs — a rename at the reader boundary).
 *
 * <p><b>Type is a plain String here on purpose.</b> {@link Dataset} lives in {@code server} and must not
 * depend on the ES|QL {@code DataType} enum (an x-pack type). The String is validated against the set of
 * types the relevant format reader can produce, and resolved to a {@code DataType}, in the ES|QL layer at
 * dataset-put validation and at query-time schema resolution — not here.
 */
public final class DatasetFieldMapping implements Writeable, ToXContentObject {

    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField SOURCE = new ParseField("source");

    private static final ConstructingObjectParser<DatasetFieldMapping, Void> PARSER = new ConstructingObjectParser<>(
        "dataset_field_mapping",
        false,
        args -> new DatasetFieldMapping((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), TYPE);
        PARSER.declareString(optionalConstructorArg(), SOURCE);
    }

    private final String type;
    @Nullable
    private final String source;

    public DatasetFieldMapping(String type, @Nullable String source) {
        this.type = Objects.requireNonNull(type, "field mapping type must not be null");
        this.source = source;
    }

    public DatasetFieldMapping(StreamInput in) throws IOException {
        this.type = in.readString();
        this.source = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(source);
    }

    public static DatasetFieldMapping fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        if (source != null) {
            builder.field(SOURCE.getPreferredName(), source);
        }
        builder.endObject();
        return builder;
    }

    /** The declared ES|QL type name (e.g. {@code "keyword"}, {@code "long"}). Validated/resolved in the ES|QL layer. */
    public String type() {
        return type;
    }

    /** The physical column name in the file, or {@code null} when the logical name equals the physical name. */
    @Nullable
    public String source() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetFieldMapping that = (DatasetFieldMapping) o;
        return type.equals(that.type) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, source);
    }

    @Override
    public String toString() {
        return "DatasetFieldMapping[type=" + type + (source != null ? ", source=" + source : "") + "]";
    }
}
