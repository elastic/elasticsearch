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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Field attribute for {@code _timeseries} field
 */
public final class TimeSeriesMetadataAttribute extends FieldAttribute {
    private final Set<String> withoutFields;

    public TimeSeriesMetadataAttribute(Source source, Set<String> withoutFields) {
        this(source, null, null, MetadataAttribute.TIMESERIES, timeSeriesField(), Nullability.TRUE, null, false, withoutFields);
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
        Set<String> withoutFields
    ) {
        super(source, parentName, qualifier, name, field, nullability, id, synthetic);
        this.withoutFields = asImmutableSet(withoutFields);
    }

    private static Set<String> asImmutableSet(Set<String> without) {
        if (without.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(without));
    }

    /**
     * Builds a {@code _timeseries} attribute from an existing attribute while preserving its identity.
     */
    public static TimeSeriesMetadataAttribute from(Attribute attribute, Set<String> withoutFields) {
        if (attribute instanceof TimeSeriesMetadataAttribute timeSeriesAttribute
            && timeSeriesAttribute.withoutFields.equals(withoutFields)) {
            return timeSeriesAttribute;
        }
        String parentName = attribute instanceof FieldAttribute fieldAttribute ? fieldAttribute.parentName() : null;
        return new TimeSeriesMetadataAttribute(
            attribute.source(),
            parentName,
            attribute.qualifier(),
            MetadataAttribute.TIMESERIES,
            timeSeriesField(),
            attribute.nullable(),
            attribute.id(),
            attribute.synthetic(),
            withoutFields
        );
    }

    public Set<String> withoutFields() {
        return withoutFields;
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
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
            withoutFields
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
        return new TimeSeriesMetadataAttribute(source, parentName(), qualifier, name, field(), nullability, id, synthetic, withoutFields);
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), withoutFields);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        TimeSeriesMetadataAttribute other = (TimeSeriesMetadataAttribute) o;
        return super.innerEquals(other, ignoreIds) && Objects.equals(withoutFields, other.withoutFields);
    }
}
