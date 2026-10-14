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
 * <p>{@code rename_from} names the file column this declared column reads from, and renaming is what it does: the
 * file column becomes this one logical column and its own name is gone from the schema (1:1, enforced at
 * dataset-put). E.g. {@code "@timestamp": {"type":"date","rename_from":"ts"}} leaves no {@code ts} column to query.
 * The key is a verb because the operation is one, following the index mapping's {@code copy_to}; it is deliberately
 * not the index {@code alias} field's {@code path}, which adds a second name and leaves the target in place.
 * {@code path} is the former spelling of this key and is accepted on equal terms.
 *
 * <p><b>Binding contract per format family.</b> Both strict ({@code dynamic: false}) and non-strict resolution bind
 * a declared column to the file column of the same physical name. For <em>headered text</em> formats (CSV/TSV) the
 * physical name is the header column name; for <em>headerless</em> text the physical name is the positional alias
 * the reader assigns ({@code col0}, {@code col1}, …), so declared names there still bind by name. For
 * <em>columnar</em> formats (Parquet/ORC) the physical name comes from the footer. A declared name the file does
 * not supply reads null with a warning, under either {@code dynamic} value. File columns left undeclared keep
 * their inferred type under {@code dynamic: true} and are unread under {@code dynamic: false}. For columnar formats
 * a declared type the resolved schema's type cannot be read as is rejected at resolution. Otherwise a declared type
 * the file's values cannot be read as follows the dataset's {@code error_mode}: per value, for every format,
 * when the read-time coercion fails on a value; and for columnar formats also per column of one file, when that
 * file's type cannot be read as declared at all (fail the query, null the column, or skip the file's rows).
 *
 * <p><b>Type is a plain String here on purpose.</b> {@link Dataset} lives in {@code server} and must not
 * depend on the ES|QL {@code DataType} enum (an x-pack type). The String is validated against the set of
 * types the relevant format reader can produce, and resolved to a {@code DataType}, in the ES|QL layer at
 * dataset-put validation and at query-time schema resolution — not here.
 */
public final class DatasetFieldMapping implements Writeable, ToXContentObject {

    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField RENAME_FROM = new ParseField("rename_from");
    /**
     * The former spelling of {@link #RENAME_FROM}, accepted on equal terms and not deprecated: datasets registered
     * under it keep working and keep being written by clients that still use it. Declared as its own field rather
     * than as a deprecated name on {@code rename_from} because a deprecated name always warns, and two names for one
     * setting are not a mistake worth warning about. Both in one entry is, and {@link #collapse} rejects it.
     */
    private static final ParseField PATH = new ParseField("path");
    private static final ParseField FORMAT = new ParseField("format");

    private static final ConstructingObjectParser<DatasetFieldMapping, Void> PARSER = new ConstructingObjectParser<>(
        "dataset_field_mapping",
        false,
        args -> collapse((String) args[0], (String) args[1], (String) args[2], (String) args[3])
    );

    static {
        PARSER.declareString(constructorArg(), TYPE);
        PARSER.declareString(optionalConstructorArg(), RENAME_FROM);
        PARSER.declareString(optionalConstructorArg(), PATH);
        // date-parse pattern for a declared `date` column (the index date-field `format` shape); type/pattern validity
        // is checked in the ES|QL layer at dataset-put, not here.
        PARSER.declareString(optionalConstructorArg(), FORMAT);
    }

    private final String type;
    @Nullable
    private final String renameFrom;
    /**
     * Date-parse pattern for a declared {@code date} column (the index date-field {@code format} shape), or {@code null}.
     * Only valid on a column whose type resolves to {@code datetime}; the type/pattern check happens in the ES|QL layer
     * at dataset-put (this class stays shape-only). Consumed by the text readers to parse timestamps for this column.
     */
    @Nullable
    private final String format;

    /** Convenience: a column with no declared date {@code format}. */
    public DatasetFieldMapping(String type, @Nullable String renameFrom) {
        this(type, renameFrom, (String) null);
    }

    /**
     * Folds the two spellings of the rename setting into one value. They mean the same thing, so either alone is
     * accepted; both in one entry is a contradiction in the making — nothing says which wins, and a later reader of
     * the stored definition would have to guess — so it is refused at the boundary rather than resolved by precedence.
     * <p>
     * {@code PARSER.declareExclusiveFieldSet} would also express mutual exclusion but its message names the fields
     * without saying they are the same setting. The explicit check here gives the caller that context.
     */
    private static DatasetFieldMapping collapse(String type, @Nullable String renameFrom, @Nullable String path, @Nullable String format) {
        if (renameFrom != null && path != null) {
            throw new IllegalArgumentException("[rename_from] and [path] are the same setting; specify one of them, not both");
        }
        return new DatasetFieldMapping(type, renameFrom != null ? renameFrom : path, format);
    }

    /**
     * A column with a declared date {@code format}.
     *
     * <p>This is a named factory, not a public 3-arg constructor, on purpose. The erased signature
     * {@code (String, String, String)} previously meant {@code (type, path, copyTo)} — the rename key was spelled
     * {@code path} then, and {@code copy_to} has since been
     * dropped and the third String now means {@code format}. Exposing it only as {@code withFormat} keeps the
     * argument's meaning in the call site and prevents a future third-String field from silently inheriting the old
     * positional contract.
     */
    public static DatasetFieldMapping withFormat(String type, @Nullable String renameFrom, @Nullable String format) {
        return new DatasetFieldMapping(type, renameFrom, format);
    }

    private DatasetFieldMapping(String type, @Nullable String renameFrom, @Nullable String format) {
        this.type = Objects.requireNonNull(type, "field mapping type must not be null");
        this.renameFrom = renameFrom;
        this.format = format;
    }

    public DatasetFieldMapping(StreamInput in) throws IOException {
        // Reached only under the dataset_declared_schema transport version, which gates the whole DatasetMapping.
        // The three slots are positional and have not changed since, so neither `format` nor the rename of `path` to
        // `rename_from` needs a gate of its own: a node on either side reads the same slots in the same order.
        this.type = in.readString();
        this.renameFrom = in.readOptionalString();
        this.format = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(renameFrom);
        out.writeOptionalString(format);
    }

    public static DatasetFieldMapping fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        if (renameFrom != null) {
            builder.field(RENAME_FROM.getPreferredName(), renameFrom);
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

    /** The file column this column is renamed from, or {@code null} when it reads the column of its own name. */
    @Nullable
    public String renameFrom() {
        return renameFrom;
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
        return type.equals(that.type) && Objects.equals(renameFrom, that.renameFrom) && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, renameFrom, format);
    }

    @Override
    public String toString() {
        return "DatasetFieldMapping[type="
            + type
            + (renameFrom != null ? ", renameFrom=" + renameFrom : "")
            + (format != null ? ", format=" + format : "")
            + "]";
    }
}
