/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * The result of translating a PromQL node: an ES|QL plan plus the label columns and numeric value it exposes.
 * <p>
 * The labels are the table's label columns bound to the plan attributes producing them - a {@link DynamicColumnList} column here
 * is absolute: that physical column. They are ordered as grouping keys: dynamic columns by increasing exclusions, then
 * the static columns in declaration order; every static column is functionally dependent on the finest dynamic one, so
 * grouping by all of them keeps the series grain. Invariant: every label attribute belongs to the plan's output.
 */
public record TranslationResult(
    LogicalPlan plan,
    Map<TranslationColumn, Attribute> labels,
    Expression value,
    Attribute step,
    Expression pendingFilter,
    Kind kind
) {

    public enum Kind {
        BEFORE_INITIAL_AGGREGATE(false, false),
        AFTER_INITIAL_AGGREGATE(true, false),
        CONSTANT(true, true);

        public final boolean constant;
        public final boolean afterInitialAggregation;

        Kind(boolean afterInitialAggregation, boolean constant) {
            this.afterInitialAggregation = afterInitialAggregation;
            this.constant = constant;
        }
    }

    private static final Comparator<DynamicColumnList> BY_EXCLUSIONS = Comparator.<DynamicColumnList>comparingInt(d -> d.except().size())
        .thenComparing(DynamicColumnList::name);

    public TranslationResult {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(labels, "labels");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(kind, "kind");
        labels = ordered(labels);
        assert plan.outputSet().containsAll(labels.values()) : "[INVARIANT]: label attributes must belong to the output of " + plan;
    }

    private static Map<TranslationColumn, Attribute> ordered(Map<TranslationColumn, Attribute> labels) {
        var dynamics = new TreeMap<DynamicColumnList, Attribute>(BY_EXCLUSIONS);
        var statics = new LinkedHashMap<TranslationColumn, Attribute>();
        labels.forEach((column, attribute) -> {
            assert attribute != null : "[INVARIANT]: every label column is bound: " + column;
            switch (column) {
                case DynamicColumnList d -> dynamics.put(d, attribute);
                case Static s -> statics.put(s, attribute);
            }
        });
        var ordered = new LinkedHashMap<TranslationColumn, Attribute>(dynamics);
        ordered.putAll(statics);
        return unmodifiableMap(ordered);
    }

    // ---------- factories that encode valid states ----------

    /** Scalar/local relation before any aggregation. */
    public static TranslationResult scalar(LogicalPlan plan, Expression value, Attribute step) {
        return new TranslationResult(plan, Map.of(), value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
    }

    /** Scalar with a pending label matcher filter. */
    public static TranslationResult scalar(LogicalPlan plan, Expression value, Attribute step, Expression selectorFilter) {
        return new TranslationResult(plan, Map.of(), value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
    }

    // ---------- labels ----------

    /** The value as a defined column; valid only when the value is an attribute. */
    public Attribute valueColumn() {
        return (Attribute) value;
    }

    /** A table with no rows at all - the relation of a query over no matching index; there is nothing to compute over it. */
    public boolean isEmpty() {
        return plan instanceof LocalRelation local && local.supplier() == EmptyLocalSupplier.EMPTY;
    }

    /** The attribute of a static label column, or null when the table does not carry the label. */
    public Attribute label(String name) {
        return labels.get(new Static(name));
    }

    /** The column carrying the series identity - the dynamic column with the fewest exclusions - or null when closed. */
    public Attribute grain() {
        for (var entry : labels.entrySet()) {
            if (entry.getKey() instanceof DynamicColumnList) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** The static label columns' names, in order. */
    public Set<String> statics() {
        var names = new LinkedHashSet<String>();
        for (var column : labels.keySet()) {
            if (column instanceof Static s) {
                names.add(s.name());
            }
        }
        return unmodifiableSet(names);
    }

    /** The label columns as a requirement: what a parent would have to require to get exactly these columns back. */
    public TranslationConstraint shape() {
        var names = new LinkedHashSet<String>();
        var allBut = new LinkedHashSet<Set<String>>();
        for (var column : labels.keySet()) {
            switch (column) {
                case Static s -> names.add(s.name());
                case DynamicColumnList d -> allBut.add(d.except());
            }
        }
        return new TranslationConstraint(names, allBut);
    }

    /** The label attributes in grouping-key order. */
    public List<Attribute> attributes() {
        return List.copyOf(labels.values());
    }

    // ---------- derivation ----------

    /** Rebuild around a new plan and value, keeping labels and other properties. */
    public TranslationResult with(LogicalPlan plan, Expression value) {
        return new TranslationResult(plan, labels, value, step, pendingFilter, kind);
    }

    /** Rebuild around a new plan, labels and value, keeping other properties. */
    public TranslationResult with(LogicalPlan plan, Map<TranslationColumn, Attribute> labels, Expression value) {
        return new TranslationResult(plan, labels, value, step, pendingFilter, kind);
    }

    /** This table with a label column added, or a stored one shadowed by a derived one. */
    public TranslationResult bind(String name, Attribute attribute) {
        return bind(plan, name, attribute);
    }

    /**
     * This table over {@code plan} - which defines {@code attribute} and may no longer carry the column it shadows - with
     * the label bound to it.
     */
    public TranslationResult bind(LogicalPlan plan, String name, Attribute attribute) {
        var bound = new LinkedHashMap<>(labels);
        bound.put(new Static(name), attribute);
        return new TranslationResult(plan, bound, value, step, pendingFilter, kind);
    }

    /**
     * This table below an operator that drops {@code dropped}: the labels go, and so does every dynamic column that still
     * contains one of them. The plan is unchanged; the columns simply stop being exposed.
     */
    public TranslationResult drop(Collection<String> dropped) {
        var kept = new LinkedHashMap<TranslationColumn, Attribute>();
        labels.forEach((column, attribute) -> {
            boolean survives = switch (column) {
                case Static s -> dropped.contains(s.name()) == false;
                case DynamicColumnList d -> d.except().containsAll(dropped);
            };
            if (survives) {
                kept.put(column, attribute);
            }
        });
        return new TranslationResult(plan, kept, value, step, pendingFilter, kind);
    }
}
