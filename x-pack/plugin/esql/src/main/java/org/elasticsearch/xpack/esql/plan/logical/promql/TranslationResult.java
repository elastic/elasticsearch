/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A translated table, its named label bindings, and optionally its complete current label record.
 * The record is an ordinary plan attribute, not a description of a storage projection. Transforming labels
 * must update that attribute and any named bindings together; neither the scan nor a parent reconstructs old records.
 * A missing record means that the named bindings are sufficient for this translation.
 */
public record TranslationResult(
    LogicalPlan plan,
    Map<String, Attribute> labels,
    @Nullable Attribute packedLabels,
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

    public TranslationResult {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(kind, "kind");
        labels = Collections.unmodifiableMap(new LinkedHashMap<>(labels));
        assert labels.values().stream().allMatch(Objects::nonNull) : "every named label must be bound";
        assert plan.outputSet().containsAll(labels.values()) : "named labels must belong to the plan output";
        assert packedLabels == null || plan.outputSet().contains(packedLabels) : "packed labels must belong to the plan output";
    }

    /** Creates a table that does not need an open-schema label record. */
    public TranslationResult(
        LogicalPlan plan,
        Map<String, Attribute> labels,
        Expression value,
        Attribute step,
        Expression pendingFilter,
        Kind kind
    ) {
        this(plan, labels, null, value, step, pendingFilter, kind);
    }

    /** Scalar/local relation before any aggregation. */
    public static TranslationResult scalar(LogicalPlan plan, Expression value, Attribute step) {
        return scalar(plan, value, step, null);
    }

    /** Scalar with a pending selector filter. */
    public static TranslationResult scalar(LogicalPlan plan, Expression value, Attribute step, Expression filter) {
        return new TranslationResult(plan, Map.of(), value, step, filter, Kind.BEFORE_INITIAL_AGGREGATE);
    }

    /** The materialized value; valid after the value expression has been defined in the plan. */
    public Attribute valueColumn() {
        return (Attribute) value;
    }

    /** A table with no rows at all - the relation of a query over no matching index; there is nothing to compute over it. */
    public boolean isEmpty() {
        return plan instanceof LocalRelation local && local.supplier() == EmptyLocalSupplier.EMPTY;
    }

    /** Returns a named projection, not an arbitrary column with the same name in the plan. */
    public Attribute label(String name) {
        return labels.get(name);
    }

    /** Names available as concrete columns, in grouping order. */
    public Set<String> labelNames() {
        return labels.keySet();
    }

    /** Selects the current labels, without referring to previous storage projections. */
    public TranslationConstraint shape() {
        return packedLabels == null
            ? TranslationConstraint.of(labels.keySet())
            : TranslationConstraint.union(TranslationConstraint.any(), TranslationConstraint.of(labels.keySet()));
    }

    /** Current record first, then its named projections, preserving the existing grouping layout. */
    public List<Attribute> attributes() {
        var attributes = new ArrayList<Attribute>(labels.size() + 1);
        if (packedLabels != null) {
            attributes.add(packedLabels);
        }
        attributes.addAll(labels.values());
        return List.copyOf(attributes);
    }

    /** Changes the plan/value while preserving the label bindings. */
    public TranslationResult with(LogicalPlan plan, Expression value) {
        return with(plan, labels, packedLabels, value);
    }

    /** Rebinds both label representations after a projection or a transformation. */
    public TranslationResult with(LogicalPlan plan, Map<String, Attribute> labels, Attribute packedLabels, Expression value) {
        return new TranslationResult(plan, labels, packedLabels, value, step, pendingFilter, kind);
    }
}
