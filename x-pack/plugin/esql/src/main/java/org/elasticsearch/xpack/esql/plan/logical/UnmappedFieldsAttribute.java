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
import java.util.List;
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
 * <p>The column expands to its per-field columns only on the coordinator, long after {@code KEEP} was resolved against the
 * (then still invisible) leaves, so {@link #keepOrder()} carries the governing {@code KEEP}'s projection terms in written order.
 * The coordinator replays {@code KEEP}'s ordering over the real columns plus the discovered leaves (see
 * {@link UnmappedFieldsPattern#keepOrdered}) so the output honors the left-to-right {@code KEEP} contract; it is empty when no
 * top {@code KEEP} governs the order, in which case the leaves keep their natural (real-then-alphabetical) position.
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
    private final List<UnmappedFieldsPattern.KeepTerm> keepOrder;

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern) {
        this(source, pattern, List.of());
    }

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern, List<UnmappedFieldsPattern.KeepTerm> keepOrder) {
        super(source, ATTRIBUTE_NAME, DataType.KEYWORD, Nullability.TRUE, null, false);
        this.pattern = pattern;
        this.keepOrder = List.copyOf(keepOrder);
    }

    public UnmappedFieldsAttribute(
        Source source,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        UnmappedFieldsPattern pattern,
        List<UnmappedFieldsPattern.KeepTerm> keepOrder
    ) {
        super(source, ATTRIBUTE_NAME, type, nullability, id, synthetic);
        this.pattern = pattern;
        this.keepOrder = List.copyOf(keepOrder);
    }

    public UnmappedFieldsPattern pattern() {
        return pattern;
    }

    /**
     * The governing {@code KEEP}'s projection terms in written order (bare {@code *}, wildcard patterns and explicit names), or empty
     * when no top {@code KEEP} governs the output order. Read by the coordinator to replay {@code KEEP} ordering over the expanded leaves.
     */
    public List<UnmappedFieldsPattern.KeepTerm> keepOrder() {
        return keepOrder;
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
            out.writeCollection(keepOrder);
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
            List<UnmappedFieldsPattern.KeepTerm> keepOrder = stream.readCollectionAsList(UnmappedFieldsPattern.KeepTerm::readFrom);
            return new UnmappedFieldsAttribute(source, dataType, nullability, id, synthetic, pattern, keepOrder);
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
        return new UnmappedFieldsAttribute(source, type, nullability, id, synthetic, pattern, keepOrder);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UnmappedFieldsAttribute::new, dataType(), nullable(), id(), synthetic(), pattern, keepOrder);
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), pattern, keepOrder);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (UnmappedFieldsAttribute) o;
        return super.innerEquals(other, ignoreIds) && Objects.equals(pattern, other.pattern) && Objects.equals(keepOrder, other.keepOrder);
    }
}
