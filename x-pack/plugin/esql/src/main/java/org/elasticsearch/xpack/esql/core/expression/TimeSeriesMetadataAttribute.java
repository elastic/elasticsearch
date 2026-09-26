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
import java.util.TreeSet;

/**
 * Field attribute for {@code _timeseries} field
 */
public final class TimeSeriesMetadataAttribute extends FieldAttribute {
    private final Set<String> excludedFields;

    public TimeSeriesMetadataAttribute(Source source, Set<String> excludedFields) {
        this(source, null, null, nameFor(excludedFields), timeSeriesField(), Nullability.TRUE, null, false, excludedFields);
    }

    /**
     * The attribute name of the {@code _timeseries} packing that excludes {@code excludedFields}: {@code _timeseries} when nothing
     * is excluded, otherwise {@code _timeseries$a$b} over the sorted exclusions. Distinct exclusions give distinct names, so one
     * relation can carry several packings side by side; {@link MetadataAttribute#isTimeSeriesAttributeName} recognizes them all.
     */
    public static String nameFor(Set<String> excludedFields) {
        var suffix = String.join(SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR, new TreeSet<>(excludedFields));
        return MetadataAttribute.TIMESERIES + (suffix.isEmpty() ? suffix : SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR + suffix);
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
