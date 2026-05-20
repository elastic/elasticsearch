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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

/**
 * Virtual column for external data sources (file metadata, partition columns).
 * Behaves like a regular column for resolution and filtering but is excluded
 * from wildcard ({@code *}) expansion — users must name it explicitly.
 * <p>
 * Mirrors {@link MetadataAttribute}'s shape but extends {@link TypedAttribute}
 * directly because {@code MetadataAttribute} is {@code final}. Implements the
 * {@link VirtualAttribute} marker so format-level pushdown rules (filter and aggregate)
 * reject it - values are materialized by {@code VirtualColumnIterator} on the
 * producer thread and have no presence in the underlying file's schema.
 */
public class ExternalMetadataAttribute extends TypedAttribute implements VirtualAttribute {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "ExternalMetadataAttribute",
        ExternalMetadataAttribute::readFrom
    );

    public ExternalMetadataAttribute(Source source, String name, DataType dataType) {
        this(source, name, dataType, Nullability.TRUE, null, false);
    }

    public ExternalMetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        super(source, name, dataType, nullability, id, synthetic);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
        }
    }

    public static ExternalMetadataAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(stream -> {
            Source source = Source.readFrom((PlanStreamInput) stream);
            String name = stream.readString();
            DataType dataType = DataType.readFrom(stream);
            Nullability nullability = stream.readEnum(Nullability.class);
            NameId id = NameId.readFrom((PlanStreamInput) stream);
            boolean synthetic = stream.readBoolean();
            return new ExternalMetadataAttribute(source, name, dataType, nullability, id, synthetic);
        });
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected ExternalMetadataAttribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new ExternalMetadataAttribute(source, name, type, nullability, id, synthetic);
    }

    @Override
    protected String label() {
        return "xm";
    }

    @Override
    public boolean isDimension() {
        return false;
    }

    @Override
    public boolean isMetric() {
        return false;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ExternalMetadataAttribute::new, name(), dataType(), nullable(), id(), synthetic());
    }
}
