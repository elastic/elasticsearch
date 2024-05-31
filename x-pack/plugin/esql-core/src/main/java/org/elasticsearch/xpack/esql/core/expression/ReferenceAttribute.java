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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;

/**
 * Attribute based on a reference to an expression.
 */
public class ReferenceAttribute extends TypedAttribute {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "ReferenceAttribute",
        ReferenceAttribute::new
    );

    public ReferenceAttribute(Source source, String name, DataType dataType) {
        this(source, name, dataType, null, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        super(source, name, dataType, qualifier, nullability, id, synthetic);
    }

    @SuppressWarnings("unchecked")
    public <S extends StreamInput & PlanStreamInput> ReferenceAttribute(StreamInput in) throws IOException {
        this(
            Source.readFrom((S) in),
            in.readString(),
            DataType.readFrom(in),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            NameId.readFrom((S) in),
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
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new ReferenceAttribute(source, name, dataType, qualifier, nullability, id, synthetic);
    }

    @Override
    protected NodeInfo<ReferenceAttribute> info() {
        return NodeInfo.create(this, ReferenceAttribute::new, name(), dataType(), qualifier(), nullable(), id(), synthetic());
    }

    @Override
    protected String label() {
        return "r";
    }
}
