/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.UnaryAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.FieldSemantics;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Links the columns of a plan back to the {@link FieldSemantics} of the mapped fields they read.
 * <p>
 * A column keeps its semantics when it passes through unchanged or is renamed. Computed columns
 * have none. Columns read from a dataset are linked to their dataset column instead.
 */
public final class ColumnSemantics {

    private ColumnSemantics() {}

    /**
     * Returns the semantics of every attribute in {@code plan} that reads a mapped field, keyed by attribute id.
     */
    public static Map<NameId, FieldSemantics> collect(LogicalPlan plan, Collection<IndexResolution> resolutions) {
        Map<String, EsIndex> indices = new HashMap<>();
        for (IndexResolution resolution : resolutions) {
            if (resolution != null && resolution.isValid()) {
                indices.put(resolution.get().name(), resolution.get());
            }
        }
        Map<NameId, FieldSemantics> semantics = new HashMap<>();
        if (indices.isEmpty()) {
            return semantics;
        }
        plan.forEachUp(EsRelation.class, relation -> {
            EsIndex index = indices.get(relation.indexPattern());
            if (index == null) {
                return;
            }
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field) {
                    FieldSemantics fieldSemantics = index.fieldSemantics().get(field.fieldName().string());
                    if (fieldSemantics != null) {
                        semantics.put(field.id(), fieldSemantics);
                    }
                }
            }
        });
        followAliases(plan, semantics);
        return semantics;
    }

    /**
     * Returns the dataset column that every attribute in {@code plan} reads, keyed by attribute id. A dataset has a
     * schema but no mapping, so the column is all that is known.
     */
    public static Map<NameId, Attribute> datasetColumns(LogicalPlan plan) {
        Map<NameId, Attribute> columns = new HashMap<>();
        plan.forEachUp(ExternalRelation.class, relation -> {
            for (Attribute attribute : relation.output()) {
                columns.put(attribute.id(), attribute);
            }
        });
        followAliases(plan, columns);
        return columns;
    }

    /**
     * RENAME and pass-through aliases keep what is known about the column they point at, and so do the grouping keys
     * of a translated {@code TS} aggregation, which are read per series and may be packed into one column.
     */
    private static <T> void followAliases(LogicalPlan plan, Map<NameId, T> known) {
        // Grouping keys read per series, and packed dimensions, by the attribute that holds them.
        Map<NameId, NameId> groupingValues = new HashMap<>();
        Map<NameId, List<NameId>> packed = new HashMap<>();
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                Expression child = alias.child();
                if (child instanceof Attribute source) {
                    copy(known, source.id(), alias.id());
                    List<NameId> dims = packed.get(source.id());
                    if (dims != null) {
                        packed.put(alias.id(), dims);
                    }
                } else if (node instanceof TimeSeriesAggregate) {
                    if ((child instanceof Values || child instanceof DimensionValues)
                        && ((UnaryAggregateFunction) child).field() instanceof Attribute field) {
                        groupingValues.put(alias.id(), field.id());
                        if (child instanceof DimensionValues) {
                            copy(known, field.id(), alias.id());
                        }
                    } else if (child instanceof PackDimsAgg pack) {
                        packed.put(alias.id(), pack.fields().stream().map(f -> ((Attribute) f).id()).toList());
                    }
                }
            });
            if (node instanceof Aggregate aggregate && (node instanceof TimeSeriesAggregate) == false) {
                for (Expression grouping : aggregate.groupings()) {
                    if (grouping instanceof Alias alias && alias.child() instanceof Attribute values) {
                        NameId field = groupingValues.get(values.id());
                        if (field != null) {
                            copy(known, field, alias.id());
                        }
                    }
                }
            } else if (node instanceof PackDims pack) {
                packed.put(pack.packed().id(), pack.dims().stream().map(Attribute::id).toList());
            } else if (node instanceof UnpackDims unpack) {
                List<NameId> dims = packed.get(unpack.packed().id());
                if (dims != null && dims.size() == unpack.dims().size()) {
                    for (int i = 0; i < dims.size(); i++) {
                        copy(known, dims.get(i), unpack.dims().get(i).id());
                    }
                }
            }
        });
    }

    private static <T> void copy(Map<NameId, T> known, NameId from, NameId to) {
        T value = known.get(from);
        if (value != null) {
            known.put(to, value);
        }
    }
}
