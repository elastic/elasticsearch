/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Objects;
import java.util.Set;

/**
 * Field attribute for {@code _timeseries} field
 */
public final class TimeSeriesMetadataAttribute extends FieldAttribute {
    private final Set<String> excludedFields;

    public TimeSeriesMetadataAttribute(Source source, Set<String> excludedFields) {
        this(source, null, null, MetadataAttribute.TIMESERIES, timeSeriesField(), Nullability.TRUE, null, false, excludedFields);
    }

    public TimeSeriesMetadataAttribute(
        Source source,
        @Nullable String parentName,
        @Nullable String qualifier,
        String name,
        EsField field,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        Set<String> excludedFields
    ) {
        super(source, parentName, qualifier, name, field, nullability, id, synthetic);
        this.excludedFields = excludedFields;
    }

    public Set<String> excludedFields() {
        return excludedFields;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            TimeSeriesMetadataAttribute::new,
            parentName(),
            qualifier(),
            name(),
            field(),
            nullable(),
            id(),
            synthetic(),
            excludedFields
        );
    }

    @Override
    protected Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        // Ignore `type`, this must be the same as the field's type.
        return new TimeSeriesMetadataAttribute(source, parentName(), qualifier, name, field(), nullability, id, synthetic, excludedFields);
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), excludedFields);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (TimeSeriesMetadataAttribute) o;
        return super.innerEquals(other, ignoreIds) && Objects.equals(excludedFields, other.excludedFields);
    }
}
