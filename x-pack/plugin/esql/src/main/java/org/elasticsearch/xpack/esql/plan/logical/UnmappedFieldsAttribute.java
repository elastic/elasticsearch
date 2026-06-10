/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Objects;

/**
 * A planning-time-only {@link MetadataAttribute} that annotates an {@link EsRelation} with
 * the {@link UnmappedFieldsPattern} describing which additional unmapped source fields would
 * survive to the query output when {@code SET unmapped_fields="LOAD_ALL"} is in effect.
 *
 * <p>This attribute is added to {@link EsRelation#output()} by
 * {@code DetermineUnmappedFieldsToKeep} in the Finish Analysis batch and is never serialized
 * or executed.
 */
public final class UnmappedFieldsAttribute extends MetadataAttribute {

    public static final String ATTRIBUTE_NAME = "_unmapped_fields";

    private final UnmappedFieldsPattern pattern;

    public UnmappedFieldsAttribute(Source source, UnmappedFieldsPattern pattern) {
        super(source, ATTRIBUTE_NAME, DataType.NULL, false);
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
        super(source, ATTRIBUTE_NAME, type, nullability, id, synthetic, false);
        this.pattern = pattern;
    }

    /** The unmapped-fields pattern this attribute carries. */
    public UnmappedFieldsPattern pattern() {
        return pattern;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("planning-time only");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("planning-time only");
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
