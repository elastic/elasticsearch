/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Field attribute for {@code _timeseries} field
 */
public final class TimeSeriesMetadataAttribute extends FieldAttribute {
    private final Set<String> excludedFields;

    private TimeSeriesMetadataAttribute(
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
        if (field.getDataType() != DataType.KEYWORD && field.getDataType() != DataType.TIMESERIES_DATA_TYPE) {
            throw new IllegalArgumentException("Unsupported time-series metadata type [" + field.getDataType() + "]");
        }
        this.excludedFields = Set.copyOf(excludedFields);
    }

    /** Create legacy metadata when the caller has not selected the typed metadata path. */
    public static TimeSeriesMetadataAttribute create(Source source, Set<String> excludedFields) {
        return create(source, excludedFields, null);
    }

    /** Create metadata using the version supported by every participating node, or legacy metadata when no version is supplied. */
    public static TimeSeriesMetadataAttribute create(Source source, Set<String> excludedFields, @Nullable TransportVersion minimumVersion) {
        return create(source, excludedFields, minimumVersion, Nullability.TRUE, false);
    }

    /** Create metadata while preserving the grouping's nullability and synthetic status. */
    public static TimeSeriesMetadataAttribute create(
        Source source,
        Set<String> excludedFields,
        @Nullable TransportVersion minimumVersion,
        Nullability nullability,
        boolean synthetic
    ) {
        EsField field = supportTimeSeriesMetadataType(minimumVersion)
            ? new EsField(
                MetadataAttribute.TIMESERIES,
                DataType.TIMESERIES_DATA_TYPE,
                Map.of(),
                false,
                EsField.TimeSeriesFieldType.DIMENSION
            )
            : timeSeriesField();
        return new TimeSeriesMetadataAttribute(
            source,
            null,
            null,
            MetadataAttribute.TIMESERIES,
            field,
            nullability,
            null,
            synthetic,
            excludedFields
        );
    }

    /** Whether typed metadata is available; a null version leaves the caller on the legacy path. */
    public static boolean supportTimeSeriesMetadataType(@Nullable TransportVersion minimumVersion) {
        return minimumVersion != null
            && DataType.TIMESERIES_DATA_TYPE.supportedVersion().supportedOn(minimumVersion, Build.current().isSnapshot());
    }

    /** Restore the existing field-attribute wire representation without exposing arbitrary field construction to callers. */
    static TimeSeriesMetadataAttribute fromField(FieldAttribute attribute, Set<String> excludedFields) {
        return new TimeSeriesMetadataAttribute(
            attribute.source(),
            attribute.parentName(),
            attribute.qualifier(),
            attribute.name(),
            attribute.field(),
            attribute.nullable(),
            attribute.id(),
            attribute.synthetic(),
            excludedFields
        );
    }

    public Set<String> excludedFields() {
        return excludedFields;
    }

    /** Read metadata from source after aggregation, preserving its representation and assigning a fresh identity to the deferred read. */
    public TimeSeriesMetadataAttribute forSourceRead() {
        return new TimeSeriesMetadataAttribute(
            source(),
            null,
            qualifier(),
            name(),
            new FunctionEsField(
                new EsField(SourceFieldMapper.NAME, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION),
                dataType(),
                new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, excludedFields)
            ),
            nullable(),
            null,
            true,
            excludedFields
        );
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
