/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * The synthetic {@code $$unmapped_fields} column produced when {@code SET unmapped_fields="LOAD_ALL"}
 * is in effect.
 *
 * <p>Added to {@link EsRelation#output()} by {@code DetermineUnmappedFieldsToKeep} in the
 * Finish Analysis batch. The carried {@link UnmappedFieldsPattern} describes which additional
 * (currently unmapped) source fields are loaded into the JSON object value of the column.
 *
 * <p>Note that {@link #synthetic()} stays {@code false} despite the synthetic name:
 * {@code Analyzer.planWithoutSyntheticAttributes} projects away every attribute for which it returns
 * {@code true}, and it runs after {@code DetermineUnmappedFieldsToKeep}, so the column would be gone
 * before the coordinator ever got to expand it. The {@code $$} name is what keeps the column
 * unreachable from a query — user field names cannot start with it.
 *
 * <p>The column carries only the pattern, which is the one thing a data node reads. Where the discovered fields end up in the
 * output is decided on the coordinator by {@code UnmappedFieldsOrdering}, which replays the plan over them - so no ordering
 * state rides along to the data nodes.
 */
public final class UnmappedFieldsAttribute extends TypedAttribute {
    public static final String ATTRIBUTE_NAME = Attribute.rawTemporaryName("unmapped_fields");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "UnmappedFieldsAttribute",
        UnmappedFieldsAttribute::readFrom
    );
    public static final NamedWriteableRegistry.Entry NAMED_EXPRESSION_ENTRY = new NamedWriteableRegistry.Entry(
        NamedExpression.class,
        ENTRY.name,
        UnmappedFieldsAttribute::readFrom
    );
    public static final NamedWriteableRegistry.Entry EXPRESSION_ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        ENTRY.name,
        UnmappedFieldsAttribute::readFrom
    );

    private final UnmappedFieldsPattern pattern;

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern) {
        super(source, ATTRIBUTE_NAME, DataType.KEYWORD, Nullability.TRUE, null, false);
        this.pattern = pattern;
    }

    public UnmappedFieldsAttribute(
        Source source,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        UnmappedFieldsPattern pattern
    ) {
        super(source, ATTRIBUTE_NAME, type, nullability, id, synthetic);
        this.pattern = pattern;
    }

    public UnmappedFieldsPattern pattern() {
        return pattern;
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
            out.writeNamedWriteable(pattern);
        }
    }

    public static UnmappedFieldsAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(stream -> {
            Source source = Source.readFrom((PlanStreamInput) stream);
            stream.readString(); // attribute name, always ATTRIBUTE_NAME
            DataType dataType = DataType.readFrom(stream);
            stream.readOptionalString(); // qualifier, no longer used
            Nullability nullability = stream.readEnum(Nullability.class);
            NameId id = NameId.readFrom((PlanStreamInput) stream);
            boolean synthetic = stream.readBoolean();
            UnmappedFieldsPattern pattern = stream.readNamedWriteable(UnmappedFieldsPattern.class);
            return new UnmappedFieldsAttribute(source, dataType, nullability, id, synthetic, pattern);
        });
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected String label() {
        return "m";
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
    protected UnmappedFieldsAttribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new UnmappedFieldsAttribute(source, type, nullability, id, synthetic, pattern);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UnmappedFieldsAttribute::new, dataType(), nullable(), id(), synthetic(), pattern);
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), pattern);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (UnmappedFieldsAttribute) o;
        return super.innerEquals(other, ignoreIds) && Objects.equals(pattern, other.pattern);
    }
}
