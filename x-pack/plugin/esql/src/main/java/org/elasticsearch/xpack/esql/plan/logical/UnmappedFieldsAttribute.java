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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link MetadataAttribute} that represents the synthetic {@code _unmapped_fields} column
 * produced when {@code SET unmapped_fields="LOAD_ALL"} is in effect.
 *
 * <p>Added to {@link EsRelation#output()} by {@code DetermineUnmappedFieldsToKeep} in the
 * Finish Analysis batch. The carried {@link UnmappedFieldsPattern} describes which additional
 * (currently unmapped) source fields are loaded into the JSON object value of the column.
 */
public final class UnmappedFieldsAttribute extends MetadataAttribute {

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

    /**
     * Whether the coordinator should expand this synthetic column into per-field columns after collecting results.
     * This flag is coordinator-local: it is set by {@code DetermineUnmappedFieldsToKeep} when
     * {@code SET unmapped_fields="LOAD_ALL_EXPAND"} is active, and intentionally not serialized to data nodes
     * (which only need to produce the raw {@code _unmapped_fields} JSON column, regardless of expand mode).
     */
    private final boolean expand;
    private final UnmappedFieldsPattern pattern;

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern) {
        this(source, pattern, false);
    }

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern, boolean expand) {
        super(source, ATTRIBUTE_NAME, DataType.KEYWORD, false);
        this.expand = expand;
        this.pattern = pattern;
    }

    /** Used by deserialization ({@link #readFrom}) and {@link #clone}. */
    private UnmappedFieldsAttribute(
        Source source,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        boolean expand,
        UnmappedFieldsPattern pattern
    ) {
        super(source, ATTRIBUTE_NAME, type, nullability, id, synthetic, false);
        this.expand = expand;
        this.pattern = pattern;
    }

    /** Used by deserialization ({@link #readFrom}). {@code expand} is always {@code false} since it is not serialized. */
    public UnmappedFieldsAttribute(
        Source source,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        UnmappedFieldsPattern pattern
    ) {
        this(source, type, nullability, id, synthetic, false, pattern);
    }

    /** Returns {@code true} if the coordinator should expand this column into one column per unique field name. */
    public boolean expand() {
        return expand;
    }

    /** The unmapped-fields pattern this attribute carries. */
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
            out.writeStringCollection(pattern.includes());
            out.writeStringCollection(pattern.excludes());
        }
    }

    public static UnmappedFieldsAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(stream -> {
            Source source = Source.readFrom((PlanStreamInput) stream);
            stream.readString(); // attribute name constant _unmapped_fields
            DataType dataType = DataType.readFrom(stream);
            stream.readOptionalString(); // qualifier, no longer used
            Nullability nullability = stream.readEnum(Nullability.class);
            NameId id = NameId.readFrom((PlanStreamInput) stream);
            boolean synthetic = stream.readBoolean();
            List<String> includes = stream.readStringCollectionAsList();
            List<String> excludes = stream.readStringCollectionAsList();
            return new UnmappedFieldsAttribute(source, dataType, nullability, id, synthetic, new UnmappedFieldsPattern(includes, excludes));
        });
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return new UnmappedFieldsAttribute(source, type, nullability, id, synthetic, expand, pattern);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            (src, dt, nu, ni, sy, pa) -> new UnmappedFieldsAttribute(src, dt, nu, ni, sy, expand, pa),
            dataType(),
            nullable(),
            id(),
            synthetic(),
            pattern
        );
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), expand, pattern);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (UnmappedFieldsAttribute) o;
        return super.innerEquals(other, ignoreIds) && expand == other.expand && Objects.equals(pattern, other.pattern);
    }
}
