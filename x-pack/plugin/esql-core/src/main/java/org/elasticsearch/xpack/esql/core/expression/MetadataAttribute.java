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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Tuple.tuple;

public class MetadataAttribute extends TypedAttribute {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "MetadataAttribute",
        MetadataAttribute::new
    );

    private static final Map<String, Tuple<DataType, Boolean>> ATTRIBUTES_MAP = Map.of(
        "_version",
        tuple(DataTypes.LONG, false), // _version field is not searchable
        "_index",
        tuple(DataTypes.KEYWORD, true),
        IdFieldMapper.NAME,
        tuple(DataTypes.KEYWORD, false), // actually searchable, but fielddata access on the _id field is disallowed by default
        IgnoredFieldMapper.NAME,
        tuple(DataTypes.KEYWORD, true),
        SourceFieldMapper.NAME,
        tuple(DataTypes.SOURCE, false)
    );

    private final boolean searchable;

    public MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        boolean searchable
    ) {
        super(source, name, dataType, qualifier, nullability, id, synthetic);
        this.searchable = searchable;
    }

    public MetadataAttribute(Source source, String name, DataType dataType, boolean searchable) {
        this(source, name, dataType, null, Nullability.TRUE, null, false, searchable);
    }

    @SuppressWarnings("unchecked")
    public <S extends StreamInput & PlanStreamInput> MetadataAttribute(StreamInput in) throws IOException {
        /*
         * The funny casting dance with `<S extends...>` and `(S) in` is required
         * because we're in esql-core here and the real PlanStreamInput is in
         * esql-proper. And because NamedWriteableRegistry.Entry needs StreamInput,
         * not a PlanStreamInput. And we need PlanStreamInput to handle Source
         * and NameId. This should become a hard cast when we move everything out
         * of esql-core.
         */
        this(
            Source.readFrom((S) in),
            in.readString(),
            DataType.readFrom(in),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            NameId.readFrom((S) in),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(name());
        dataType().writeTo(out);
        out.writeOptionalString(qualifier());
        out.writeEnum(nullable());
        id().writeTo(out);
        out.writeBoolean(synthetic());
        out.writeBoolean(searchable);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MetadataAttribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new MetadataAttribute(source, name, type, qualifier, nullability, id, synthetic, searchable);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MetadataAttribute::new, name(), dataType(), qualifier(), nullable(), id(), synthetic(), searchable);
    }

    public boolean searchable() {
        return searchable;
    }

    private MetadataAttribute withSource(Source source) {
        return new MetadataAttribute(source, name(), dataType(), qualifier(), nullable(), id(), synthetic(), searchable());
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
