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
 * The {@code _unmapped_fields} column produced when {@code SET unmapped_fields="LOAD_ALL"} is in effect.
 *
 * <p>Added to {@link EsRelation#output()} by {@code DetermineUnmappedFieldsToKeep} in the
 * Finish Analysis batch, when the carried {@link UnmappedFieldsPattern} can match an unmapped field. The value of the
 * column is a JSON object of the unmapped fields of a document that the pattern includes, which the shard reads. That value
 * is {@code _source}-shaped, so the column is typed {@link DataType#SOURCE}.
 *
 * <p>The column is named like the output column it becomes, but a query cannot refer to it: it is added after every
 * reference has been resolved, and {@code ResolvingProject} keeps it out of the {@code KEEP}/{@code DROP}/{@code RENAME}
 * patterns. Several places still find it by name (union and fork branches line their columns up by name, {@code EVAL}
 * shadows by name), so {@code DetermineUnmappedFieldsToKeep} does not add it to a query that has a column of that name of
 * its own: the query's column wins.
 *
 * <p>{@link #synthetic()} stays {@code false}: {@code Analyzer.planWithoutSyntheticAttributes} projects away every attribute
 * for which it returns {@code true}, and it runs after {@code DetermineUnmappedFieldsToKeep}, so the column would be gone
 * before it is returned.
 *
 * <p>The column is returned as {@code _unmapped_fields}, with the values the data nodes loaded.
 */
public final class UnmappedFieldsAttribute extends TypedAttribute {
    public static final String ATTRIBUTE_NAME = "_unmapped_fields";

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
        super(source, ATTRIBUTE_NAME, DataType.SOURCE, Nullability.TRUE, null, false);
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
            stream.readString(); // attribute name, ignored in favor of ATTRIBUTE_NAME; older nodes write $$unmapped_fields
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
    public boolean isInternal() {
        return true;
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
