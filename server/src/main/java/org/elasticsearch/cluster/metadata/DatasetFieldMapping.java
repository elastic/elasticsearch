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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * One declared column inside a dataset's {@link DatasetMapping} {@code properties} block — the value
 * of a {@code "logical_name": { ... }} entry.
 *
 * <p>Both name-remapping keys mirror the index mapping so the dataset mapping is a subset of it: {@code path} is a
 * <em>move</em> (rename) — the physical column name the value is read from, as the index {@code alias} field's
 * {@code path} names its underlying field; {@code copy_to} is a <em>copy</em> — further logical columns that also
 * receive this column's value, exactly like the index {@code copy_to}.
 *
 * <p><b>Move vs copy.</b> {@code path} is a <em>move</em>: the physical column becomes this one logical column
 * (the physical name is consumed, 1:1) — a read-path rename. {@code copy_to} keeps this column's own value
 * <em>and</em> gives the named target(s) that value — e.g. {@code "@timestamp": {"type":"date","path":"ts"}}
 * renames {@code ts}, while {@code "user_id": {"type":"keyword","copy_to":"actor"}} keeps {@code user_id} and adds
 * {@code actor}. A copy is not a second read: it is materialized as an {@code EVAL target = <this column>} above the
 * external relation (in the ES|QL analyzer), so it works for every format and never touches the read path.
 *
 * <p><b>Binding contract per format family.</b> Non-strict resolution binds a declared column to the file column of
 * the same physical name (from the header/keys). Strict resolution ({@code dynamic: false}) reads no file at
 * declaration time; for <em>text</em> formats (CSV/TSV) it then binds <em>positionally</em> — the declared names
 * replace the header's names in order (the DuckDB {@code columns=} / ClickHouse {@code structure} contract), so
 * renaming by position needs no {@code path} and declared names are deliberately not cross-checked against the
 * header. Two guards keep drifted files loud rather than silently wrong: a declaration <em>wider</em> than a file's
 * header fails at first read (the file cannot supply the declared columns), and for <em>columnar</em> formats
 * (parquet/orc, which bind by name and carry their own types) a declared type that differs from the file's
 * reconciled type is rejected at resolution. Fewer declared columns than the file has leaves the extras unread.
 *
 * <p><b>Type is a plain String here on purpose.</b> {@link Dataset} lives in {@code server} and must not
 * depend on the ES|QL {@code DataType} enum (an x-pack type). The String is validated against the set of
 * types the relevant format reader can produce, and resolved to a {@code DataType}, in the ES|QL layer at
 * dataset-put validation and at query-time schema resolution — not here.
 */
public final class DatasetFieldMapping implements Writeable, ToXContentObject {

    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField PATH = new ParseField("path");
    private static final ParseField COPY_TO = new ParseField("copy_to");
    private static final ParseField FORMAT = new ParseField("format");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DatasetFieldMapping, Void> PARSER = new ConstructingObjectParser<>(
        "dataset_field_mapping",
        false,
        args -> new DatasetFieldMapping((String) args[0], (String) args[1], (List<String>) args[2], (String) args[3])
    );

    static {
        PARSER.declareString(constructorArg(), TYPE);
        PARSER.declareString(optionalConstructorArg(), PATH);
        // copy_to accepts a single string or an array of strings (mirrors core ES copy_to).
        PARSER.declareField(optionalConstructorArg(), (p, c) -> parseStringOrStringArray(p), COPY_TO, ObjectParser.ValueType.STRING_ARRAY);
        // date-parse pattern for a declared `date` column (the index date-field `format` shape); type/pattern validity
        // is checked in the ES|QL layer at dataset-put, not here.
        PARSER.declareString(optionalConstructorArg(), FORMAT);
    }

    private static List<String> parseStringOrStringArray(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            List<String> targets = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                targets.add(parser.text());
            }
            return targets;
        }
        return List.of(parser.text());
    }

    private final String type;
    @Nullable
    private final String path;
    /** Logical columns that receive a copy of this column's value ({@code copy_to}); empty when none, never null. */
    private final List<String> copyTo;
    /**
     * Date-parse pattern for a declared {@code date} column (the index date-field {@code format} shape), or {@code null}.
     * Only valid on a column whose type resolves to {@code datetime}; the type/pattern check happens in the ES|QL layer
     * at dataset-put (this class stays shape-only). Consumed by the text readers to parse timestamps for this column.
     */
    @Nullable
    private final String format;

    /** Convenience: a column with no {@code copy_to}. */
    public DatasetFieldMapping(String type, @Nullable String path) {
        this(type, path, List.of());
    }

    /** Convenience: a single {@code copy_to} target. */
    public DatasetFieldMapping(String type, @Nullable String path, @Nullable String copyTo) {
        this(type, path, copyTo == null ? List.of() : List.of(copyTo));
    }

    /** Convenience: {@code copy_to} targets with no declared date {@code format}. */
    public DatasetFieldMapping(String type, @Nullable String path, @Nullable List<String> copyTo) {
        this(type, path, copyTo, null);
    }

    public DatasetFieldMapping(String type, @Nullable String path, @Nullable List<String> copyTo, @Nullable String format) {
        this.type = Objects.requireNonNull(type, "field mapping type must not be null");
        this.path = path;
        this.copyTo = copyTo == null ? List.of() : List.copyOf(copyTo);
        this.format = format;
    }

    public DatasetFieldMapping(StreamInput in) throws IOException {
        // Reached only under the dataset_declared_schema transport version (the whole DatasetMapping is gated),
        // which is unreleased — so copy_to and format ship in that one version with no separate gate.
        this.type = in.readString();
        this.path = in.readOptionalString();
        this.copyTo = in.readStringCollectionAsList();
        this.format = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(path);
        out.writeStringCollection(copyTo);
        out.writeOptionalString(format);
    }

    public static DatasetFieldMapping fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        if (path != null) {
            builder.field(PATH.getPreferredName(), path);
        }
        if (copyTo.isEmpty() == false) {
            builder.field(COPY_TO.getPreferredName(), copyTo);
        }
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
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
    public String path() {
        return path;
    }

    /** Logical columns that receive a copy of this column's value ({@code copy_to}); empty when none, never null. */
    public List<String> copyTo() {
        return copyTo;
    }

    /** Date-parse pattern for a declared {@code date} column, or {@code null} when none is declared. */
    @Nullable
    public String format() {
        return format;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetFieldMapping that = (DatasetFieldMapping) o;
        return type.equals(that.type)
            && Objects.equals(path, that.path)
            && copyTo.equals(that.copyTo)
            && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, path, copyTo, format);
    }

    @Override
    public String toString() {
        return "DatasetFieldMapping[type="
            + type
            + (path != null ? ", path=" + path : "")
            + (copyTo.isEmpty() ? "" : ", copyTo=" + copyTo)
            + (format != null ? ", format=" + format : "")
            + "]";
    }
}
