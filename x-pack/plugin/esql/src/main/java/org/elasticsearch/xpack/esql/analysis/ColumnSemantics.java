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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.UnaryAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;

/**
 * Links the columns of a plan back to the {@link FieldSemantics} of the mapped fields they read.
 * <p>
 * A column keeps its semantics when it passes through unchanged or is renamed. Computed columns
 * have none, except that the buckets of a time column hold time. Columns read from a dataset are
 * linked to their dataset column instead.
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
     * Returns the ids of the columns of {@code plan} that hold time but read no mapped field: the {@code @timestamp}
     * column of a dataset, and a time column rounded ({@code BUCKET}, {@code TBUCKET}, {@code TSTEP}, {@code DATE_TRUNC},
     * {@code ROUND_TO}) or converted ({@code TO_DATETIME}, {@code TO_DATE_NANOS}), nested or not. Renames keep it, and
     * so do merges whose branches all hold time.
     */
    public static Set<NameId> timeColumns(LogicalPlan plan, Map<NameId, FieldSemantics> semantics, Map<NameId, Attribute> datasetColumns) {
        Set<NameId> time = new HashSet<>();
        datasetColumns.forEach((id, column) -> {
            if (MetadataAttribute.TIMESTAMP_FIELD.equals(column.name()) && column.dataType().isDate()) {
                time.add(id);
            }
        });
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                Expression source = alias.child();
                while (source != null && (source instanceof Attribute) == false) {
                    source = timeOf(source);
                }
                if (source instanceof Attribute attribute && isTime(attribute.id(), semantics, time)) {
                    time.add(alias.id());
                }
            });
            if (node instanceof MergePlan merge) {
                forEachMergedColumn(merge, (column, sources) -> {
                    if (sources.isEmpty() == false && sources.stream().allMatch(id -> isTime(id, semantics, time))) {
                        time.add(column.id());
                    }
                });
            }
        });
        return time;
    }

    private static boolean isTime(NameId id, Map<NameId, FieldSemantics> semantics, Set<NameId> time) {
        FieldSemantics field = semantics.get(id);
        return time.contains(id) || (field != null && field.role() == FieldSemantics.Role.TIME);
    }

    /**
     * The expression whose time {@code e} keeps: what it rounds or converts, or null for any other expression.
     */
    private static Expression timeOf(Expression e) {
        if (e instanceof Bucket bucket) {
            return bucket.field();
        }
        if (e instanceof TBucket tbucket) {
            return tbucket.timestamp();
        }
        if (e instanceof TStep tstep) {
            return tstep.timestamp();
        }
        if (e instanceof DateTrunc dateTrunc) {
            return dateTrunc.field();
        }
        if (e instanceof RoundTo roundTo) {
            return roundTo.field();
        }
        if (e instanceof ToDatetime toDatetime) {
            return toDatetime.field();
        }
        if (e instanceof ToDateNanos toDateNanos) {
            return toDateNanos.field();
        }
        return null;
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
