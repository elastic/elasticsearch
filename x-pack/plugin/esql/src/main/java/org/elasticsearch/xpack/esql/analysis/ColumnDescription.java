/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.FieldSemantics;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What {@code DESCRIBE} reports about a column, other than its name and type. {@code column_metadata} reports the
 * same values under the same names, so both say the same thing about a column.
 *
 * @param role         how the field the column reads is meant to be used, or {@code null} if it is computed and not time
 * @param field        the field or dataset column the column reads, or {@code null} if it is computed
 * @param origin       where the column comes from: {@code mapped}, {@code renamed}, {@code computed}, ...
 * @param metricType   the time series metric type of the field the column reads
 * @param units        every unit of the column, empty if it is unknown
 * @param interval     the interval of a date bucket with a constant interval
 * @param timeZone     the time zone of that date bucket
 * @param descriptions every description of the field the column reads
 * @param originalTypes  the types of an unsupported column, such as the types its indices disagree on, sorted
 * @param suggestedCast  the type to cast an unsupported column to, if any
 */
public record ColumnDescription(
    @Nullable FieldSemantics.Role role,
    @Nullable String field,
    String origin,
    @Nullable String metricType,
    List<String> units,
    @Nullable String interval,
    @Nullable String timeZone,
    List<String> descriptions,
    List<String> originalTypes,
    @Nullable String suggestedCast
) {

    /**
     * Describes every column of the resolved {@code plan}, keyed by attribute id.
     */
    public static Map<NameId, ColumnDescription> collect(
        LogicalPlan plan,
        Collection<IndexResolution> resolutions,
        FoldContext foldContext
    ) {
        Map<NameId, FieldSemantics> semantics = ColumnSemantics.collect(plan, resolutions);
        Map<NameId, Attribute> datasetColumns = ColumnSemantics.datasetColumns(plan);
        Set<NameId> timeColumns = ColumnSemantics.timeColumns(plan, semantics, datasetColumns);
        Map<NameId, List<String>> units = ColumnUnits.collect(plan, semantics);
        Map<NameId, ColumnIntervals.Interval> intervals = ColumnIntervals.collect(plan, foldContext);
        Map<NameId, ColumnDescription> descriptions = new HashMap<>();
        for (Attribute column : plan.output()) {
            FieldSemantics field = semantics.get(column.id());
            Attribute datasetColumn = datasetColumns.get(column.id());
            ColumnIntervals.Interval interval = intervals.get(column.id());
            List<String> originalTypes = originalTypes(column);
            DataType suggestedCast = originalTypes.isEmpty() ? null : DataType.suggestedCastOfTypeNames(originalTypes);
            descriptions.put(
                column.id(),
                new ColumnDescription(
                    role(column, field, timeColumns),
                    field != null ? field.field() : datasetColumn != null ? datasetColumn.name() : null,
                    origin(column, field, datasetColumn),
                    field == null ? null : field.metricType(),
                    units.getOrDefault(column.id(), List.of()),
                    interval == null ? null : interval.interval(),
                    interval == null ? null : interval.timeZone(),
                    field == null ? List.of() : List.copyOf(field.descriptions()),
                    originalTypes,
                    suggestedCast == null ? null : suggestedCast.typeName()
                )
            );
        }
        return descriptions;
    }

    /**
     * The {@code _meta} entries of a result column, named like the columns of {@code DESCRIBE}. Unknown values are left out,
     * and a list with a single value is reported as that value. The original types and the suggested cast are left out
     * too, since the column reports them itself.
     */
    public Map<String, Object> toColumnMetadata() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (role != null) {
            map.put("role", role.label());
        }
        if (field != null) {
            map.put("field", field);
        }
        map.put("origin", origin);
        if (metricType != null) {
            map.put("metric_type", metricType);
        }
        if (units.isEmpty() == false) {
            map.put("unit", FieldSemantics.singleOrList(units));
        }
        if (interval != null) {
            map.put("interval", interval);
        }
        if (timeZone != null) {
            map.put("time_zone", timeZone);
        }
        if (descriptions.isEmpty() == false) {
            map.put("description", FieldSemantics.singleOrList(descriptions));
        }
        return map;
    }

    /**
     * The types the response reports as {@code original_types} for this column, sorted, or empty if it is supported.
     */
    private static List<String> originalTypes(Attribute column) {
        Collection<String> types;
        if (column instanceof UnsupportedAttribute unsupported) {
            types = unsupported.originalTypes();
        } else if (column instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField conflict) {
            types = conflict.getTypesToIndices().keySet();
        } else if (column instanceof FieldAttribute fa && fa.field() instanceof UnsupportedEsField unsupported) {
            types = unsupported.getOriginalTypes();
        } else {
            return List.of();
        }
        List<String> sorted = new ArrayList<>(types == null ? List.of() : types);
        Collections.sort(sorted);
        return sorted;
    }

    private static String origin(Attribute column, @Nullable FieldSemantics field, @Nullable Attribute datasetColumn) {
        if (isMetadata(column)) {
            return "metadata";
        }
        if (column instanceof UnsupportedAttribute) {
            return "unsupported";
        }
        if (column instanceof FieldAttribute) {
            return field == null ? "unmapped" : "mapped";
        }
        // A column of a merge of branches is not the field itself, but keeps its name unless it was renamed.
        if (datasetColumn != null) {
            return datasetColumn.name().equals(column.name()) ? "dataset" : "renamed";
        }
        if (field == null) {
            return "computed";
        }
        return field.field().equals(column.name()) ? "mapped" : "renamed";
    }

    @Nullable
    private static FieldSemantics.Role role(Attribute column, @Nullable FieldSemantics field, Set<NameId> timeColumns) {
        if (field != null) {
            return field.role();
        }
        if (isMetadata(column)) {
            return FieldSemantics.Role.METADATA;
        }
        if (timeColumns.contains(column.id())) {
            return FieldSemantics.Role.TIME;
        }
        return null;
    }

    private static boolean isMetadata(Attribute column) {
        return column instanceof MetadataAttribute || column instanceof ExternalMetadataAttribute;
    }
}
