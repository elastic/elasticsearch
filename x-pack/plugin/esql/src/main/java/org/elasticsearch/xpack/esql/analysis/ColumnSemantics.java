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
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;

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
        followAliases(plan, semantics, FieldSemantics::merge);
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
        followAliases(plan, columns, (a, b) -> a.equals(b) ? a : null);
        return columns;
    }

    /**
     * RENAME and pass-through aliases keep what is known about the column they point at, and so do the grouping keys
     * of a translated {@code TS} aggregation, which are read per series and may be packed into one column. The columns
     * of a {@link MergePlan} keep what {@code combine} makes of their branches.
     */
    private static <T> void followAliases(LogicalPlan plan, Map<NameId, T> known, BinaryOperator<T> combine) {
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
            } else if (node instanceof MergePlan merge) {
                mergeBranches(merge, known, combine);
            }
        });
    }

    /**
     * A column of a {@link MergePlan} (views next to indices, {@code FORK}, subqueries) keeps what is known about
     * the same-named column of its branches when every branch that has the column knows it and {@code combine},
     * applied pairwise, does not return null.
     */
    static <T> void mergeBranches(MergePlan merge, Map<NameId, T> known, BinaryOperator<T> combine) {
        forEachMergedColumn(merge, (column, sources) -> {
            T merged = null;
            for (NameId source : sources) {
                T value = known.get(source);
                merged = value == null ? null : merged == null ? value : combine.apply(merged, value);
                if (merged == null) {
                    return;
                }
            }
            if (merged != null) {
                known.put(column.id(), merged);
            }
        });
    }

    /**
     * Calls {@code consumer} with every column of {@code merge} and the ids of the same-named column of the branches
     * that have it.
     */
    static void forEachMergedColumn(MergePlan merge, BiConsumer<Attribute, List<NameId>> consumer) {
        List<Map<String, Attribute>> branches = new ArrayList<>(merge.children().size());
        for (LogicalPlan branch : merge.children()) {
            Map<String, Attribute> byName = new HashMap<>();
            for (Attribute a : branch.output()) {
                byName.putIfAbsent(a.name(), a);
            }
            branches.add(byName);
        }
        for (Attribute column : merge.output()) {
            List<NameId> sources = new ArrayList<>(branches.size());
            for (Map<String, Attribute> branch : branches) {
                Attribute source = branch.get(column.name());
                if (source != null) {
                    sources.add(source.id());
                }
            }
            consumer.accept(column, sources);
        }
    }

    private static <T> void copy(Map<NameId, T> known, NameId from, NameId to) {
        T value = known.get(from);
        if (value != null) {
            known.put(to, value);
        }
    }
}
