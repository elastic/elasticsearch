/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Tuple.tuple;

public class MetadataAttribute extends TypedAttribute {
    public static final String TIMESTAMP_FIELD = "@timestamp";
    public static final String TSID_FIELD = "_tsid";

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "MetadataAttribute",
        MetadataAttribute::readFrom
    );

    private static final Map<String, Tuple<DataType, Boolean>> ATTRIBUTES_MAP = Map.of(
        "_version",
        tuple(DataType.LONG, false), // _version field is not searchable
        "_index",
        tuple(DataType.KEYWORD, true),
        IdFieldMapper.NAME,
        tuple(DataType.KEYWORD, false), // actually searchable, but fielddata access on the _id field is disallowed by default
        IgnoredFieldMapper.NAME,
        tuple(DataType.KEYWORD, true),
        SourceFieldMapper.NAME,
        tuple(DataType.SOURCE, false),
        IndexModeFieldMapper.NAME,
        tuple(DataType.KEYWORD, true)
    );

    private final boolean searchable;

    public MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        boolean searchable
    ) {
        super(source, name, dataType, nullability, id, synthetic);
        this.searchable = searchable;
    }

    public MetadataAttribute(Source source, String name, DataType dataType, boolean searchable) {
        this(source, name, dataType, Nullability.TRUE, null, false, searchable);
    }

    @Deprecated
    /**
     * Old constructor from when this had a qualifier string. Still needed to not break serialization.
     */
    private MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        @Nullable String qualifier,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        boolean searchable
    ) {
        this(source, name, dataType, nullability, id, synthetic, searchable);
    }

    @SuppressWarnings("unchecked")
    private MetadataAttribute(StreamInput in) throws IOException {
        /*
         * The funny casting dance with `(StreamInput & PlanStreamInput) in` is required
         * because we're in esql-core here and the real PlanStreamInput is in
         * esql-proper. And because NamedWriteableRegistry.Entry needs StreamInput,
         * not a PlanStreamInput. And we need PlanStreamInput to handle Source
         * and NameId. This should become a hard cast when we move everything out
         * of esql-core.
         */
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readString(),
            DataType.readFrom(in),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            NameId.readFrom((StreamInput & PlanStreamInput) in),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            // We used to write the qualifier here. We can still do if needed in the future.
            out.writeOptionalString(null);
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
            out.writeBoolean(searchable);
        }
    }

    public static MetadataAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(MetadataAttribute::new);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MetadataAttribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        return new MetadataAttribute(source, name, type, null, nullability, id, synthetic, searchable);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            (source, name, dataType, qualifier, nullability, id, synthetic, searchable1) -> new MetadataAttribute(
                source,
                name,
                dataType,
                qualifier,
                nullability,
                id,
                synthetic,
                searchable1
            ),
            name(),
            dataType(),
            (String) null,
            nullable(),
            id(),
            synthetic(),
            searchable
        );
    }

    public boolean searchable() {
        return searchable;
    }

    public static MetadataAttribute create(Source source, String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? new MetadataAttribute(source, name, t.v1(), t.v2()) : null;
    }

    public static DataType dataType(String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? t.v1() : null;
    }

    public static boolean isSupported(String name) {
        return ATTRIBUTES_MAP.containsKey(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        MetadataAttribute other = (MetadataAttribute) obj;
        return searchable == other.searchable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), searchable);
    }
}
