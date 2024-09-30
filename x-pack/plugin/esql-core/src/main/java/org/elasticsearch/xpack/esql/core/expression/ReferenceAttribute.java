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
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;

/**
 * Attribute based on a reference to an expression.
 */
public class ReferenceAttribute extends TypedAttribute {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "ReferenceAttribute",
        ReferenceAttribute::readFrom
    );

    public ReferenceAttribute(Source source, String name, DataType dataType) {
        this(source, name, dataType, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(Source source, String name, DataType dataType, Nullability nullability, NameId id, boolean synthetic) {
        super(source, name, dataType, nullability, id, synthetic);
    }

    @Deprecated
    /**
     * Old constructor from when this had a qualifier string. Still needed to not break serialization.
     */
    private ReferenceAttribute(
        Source source,
        String name,
        DataType dataType,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        this(source, name, dataType, nullability, id, synthetic);
    }

    @SuppressWarnings("unchecked")
    private ReferenceAttribute(StreamInput in) throws IOException {
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
        }
    }

    public static ReferenceAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(ReferenceAttribute::new);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Attribute clone(Source source, String name, DataType dataType, Nullability nullability, NameId id, boolean synthetic) {
        return new ReferenceAttribute(source, name, dataType, null, nullability, id, synthetic);
    }

    @Override
    protected NodeInfo<ReferenceAttribute> info() {
        return NodeInfo.create(
            this,
            (source, name, dataType, qualifier, nullability, id, synthetic) -> new ReferenceAttribute(
                source,
                name,
                dataType,
                qualifier,
                nullability,
                id,
                synthetic
            ),
            name(),
            dataType(),
            (String) null,
            nullable(),
            id(),
            synthetic()
        );
    }

    @Override
    protected String label() {
        return "r";
    }
}
