/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

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

    private static final TransportVersion ESQL_QUALIFIERS_IN_ATTRIBUTES = TransportVersion.fromName("esql_qualifiers_in_attributes");

    @Deprecated
    /**
     * Only used for tests
     */
    public ReferenceAttribute(Source source, String name, DataType dataType) {
        this(source, null, name, dataType, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(Source source, @Nullable String qualifier, String name, DataType dataType) {
        this(source, qualifier, name, dataType, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        super(source, qualifier, name, dataType, nullability, id, synthetic);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            checkAndSerializeQualifier((PlanStreamOutput) out, out.getTransportVersion());
            if (out.getTransportVersion().supports(ESQL_QUALIFIERS_IN_ATTRIBUTES) == false) {
                // We used to always serialize a null qualifier here, so do the same for bwc.
                out.writeOptionalString(null);
            }
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
        }
    }

    public static ReferenceAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(ReferenceAttribute::innerReadFrom);
    }

    private static ReferenceAttribute innerReadFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        // We could cache this if we wanted to.
        String name = in.readString();
        DataType dataType = DataType.readFrom(in);
        String qualifier = readQualifier((PlanStreamInput) in, in.getTransportVersion());
        if (in.getTransportVersion().supports(ESQL_QUALIFIERS_IN_ATTRIBUTES) == false) {
            in.readOptionalString();
        }
        Nullability nullability = in.readEnum(Nullability.class);
        NameId id = NameId.readFrom((PlanStreamInput) in);
        boolean synthetic = in.readBoolean();

        return new ReferenceAttribute(source, qualifier, name, dataType, nullability, id, synthetic);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new ReferenceAttribute(source, qualifier, name, dataType, nullability, id, synthetic);
    }

    @Override
    protected NodeInfo<ReferenceAttribute> info() {
        return NodeInfo.create(this, ReferenceAttribute::new, qualifier(), name(), dataType(), nullable(), id(), synthetic());
    }

    @Override
    protected String label() {
        return "r";
    }

    @Override
    public boolean isDimension() {
        return false;
    }

    @Override
    public boolean isMetric() {
        return false;
    }
}
