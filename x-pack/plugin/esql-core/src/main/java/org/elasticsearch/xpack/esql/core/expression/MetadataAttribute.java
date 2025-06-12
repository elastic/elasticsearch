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

public class MetadataAttribute extends TypedAttribute {
    public static final String TIMESTAMP_FIELD = "@timestamp"; // this is not a true metadata attribute
    public static final String TSID_FIELD = "_tsid";
    public static final String SCORE = "_score";
    public static final String INDEX = "_index";

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "MetadataAttribute",
        MetadataAttribute::readFrom
    );

    private static final Map<String, MetadataAttributeConfiguration> ATTRIBUTES_MAP = Map.ofEntries(
        Map.entry("_version", new MetadataAttributeConfiguration(DataType.LONG, false)),
        Map.entry(INDEX, new MetadataAttributeConfiguration(DataType.KEYWORD, true)),
        // actually _id is searchable, but fielddata access on it is disallowed by default
        Map.entry(IdFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, false)),
        Map.entry(IgnoredFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)),
        Map.entry(SourceFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.SOURCE, false)),
        Map.entry(IndexModeFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)),
        Map.entry(SCORE, new MetadataAttributeConfiguration(DataType.DOUBLE, false))
    );

    private record MetadataAttributeConfiguration(DataType dataType, boolean searchable) {}

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            out.writeOptionalString(null); // qualifier, no longer used
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
            out.writeBoolean(searchable);
        }
    }

    public static MetadataAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(streamInput -> {
            Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
            String name = in.readString();
            DataType dataType = DataType.readFrom(in);
            String qualifier = in.readOptionalString(); // qualifier, no longer used
            Nullability nullability = in.readEnum(Nullability.class);
            NameId id = NameId.readFrom((StreamInput & PlanStreamInput) in);
            boolean synthetic = in.readBoolean();
            boolean searchable = in.readBoolean();
            return new MetadataAttribute(source, name, dataType, nullability, id, synthetic, searchable);
        });
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MetadataAttribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        return new MetadataAttribute(source, name, type, nullability, id, synthetic, searchable);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MetadataAttribute::new, name(), dataType(), nullable(), id(), synthetic(), searchable);
    }

    public boolean searchable() {
        return searchable;
    }

    public static MetadataAttribute create(Source source, String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? new MetadataAttribute(source, name, t.dataType(), t.searchable()) : null;
    }

    public static DataType dataType(String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? t.dataType() : null;
    }

    public static boolean isSupported(String name) {
        return ATTRIBUTES_MAP.containsKey(name);
    }

    public static boolean isScoreAttribute(Expression a) {
        return a instanceof MetadataAttribute ma && ma.name().equals(SCORE);
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), searchable);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (MetadataAttribute) o;
        return super.innerEquals(other) && searchable == other.searchable;
    }
}
