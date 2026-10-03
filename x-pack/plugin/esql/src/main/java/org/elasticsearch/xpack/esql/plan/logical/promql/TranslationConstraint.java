/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The label requirement a parent translation places on a child: the promoted labels it requires by name, each its own
 * column, and optionally the rest - every other label, encoded as time-series metadata in a {@code _timeseries} column -
 * once per exclusion set. This is strictly a top-down propagation mechanism: a parent passes a requirement down and then
 * reads whatever it needs off the child plan's output.
 * It is a selection, not an inventory of storage projections: combining requirements only combines
 * their label names and exclusion sets, without creating physical record projections. The rest may overlap the
 * promoted names.
 *
 * @param labels promoted label reads
 * @param skips exclusion sets of the rest, one per {@code _timeseries} column
 */
public record TranslationConstraint(Set<String> labels, Set<Set<String>> skips) {
    /** No columns: a scalar's constraint, and the identity of {@link #union}. */
    public static final TranslationConstraint EMPTY = new TranslationConstraint(Set.of(), Set.of());

    public TranslationConstraint {
        labels = Collections.unmodifiableSet(new LinkedHashSet<>(labels));
        var copy = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : skips) {
            copy.add(Collections.unmodifiableSet(new LinkedHashSet<>(skip)));
        }
        skips = Collections.unmodifiableSet(copy);
    }

    /** Exactly these labels, promoted: each its own column. */
    public static TranslationConstraint promoted(Collection<String> names) {
        return new TranslationConstraint(new LinkedHashSet<>(names), Set.of());
    }

    /** The rest except {@code skip}, encoded in a {@code _timeseries} column; an empty skip set is the whole rest. */
    public static TranslationConstraint rest(Collection<String> skip) {
        return new TranslationConstraint(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** The whole rest: every label, encoded in the {@code _timeseries} column. */
    public static TranslationConstraint rest() {
        return rest(Set.of());
    }

    /** Merge two constraints: promoted labels and exclusion sets of the rest combined. */
    public static TranslationConstraint union(TranslationConstraint a, TranslationConstraint b) {
        var mergedLabels = new LinkedHashSet<>(a.labels);
        mergedLabels.addAll(b.labels);
        var mergedSkips = new LinkedHashSet<>(a.skips);
        mergedSkips.addAll(b.skips);
        return new TranslationConstraint(mergedLabels, mergedSkips);
    }

    /**
     * A constraint transposed below a node that drops {@code keys}: the dropped labels are no longer promoted, and every
     * {@code _timeseries} column of the rest must already exclude them to survive the regroup.
     */
    public static TranslationConstraint subtract(TranslationConstraint constraint, Collection<String> keys) {
        var remaining = new LinkedHashSet<>(constraint.labels);
        remaining.removeAll(keys);
        var widened = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : constraint.skips) {
            var wider = new LinkedHashSet<>(skip);
            wider.addAll(keys);
            widened.add(wider);
        }
        return new TranslationConstraint(remaining, widened);
    }

    /**
     * The columns of a constraint that survive a node dropping {@code keys}: promoted labels outside the set and the
     * {@code _timeseries} columns of the rest already excluding all of it. The upward counterpart of {@link #subtract}.
     */
    public static TranslationConstraint intersect(TranslationConstraint constraint, Collection<String> keys) {
        var remaining = new LinkedHashSet<>(constraint.labels);
        remaining.removeAll(keys);
        var covering = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : constraint.skips) {
            if (skip.containsAll(keys)) {
                covering.add(skip);
            }
        }
        return new TranslationConstraint(remaining, covering);
    }

    /** Only the promoted labels among {@code names}; the rest unchanged. Trims what a child exposes to what is required. */
    public static TranslationConstraint project(TranslationConstraint constraint, Collection<String> names) {
        var retained = new LinkedHashSet<>(constraint.labels);
        retained.retainAll(names);
        return new TranslationConstraint(retained, constraint.skips);
    }

    /** True when this constraint carries the rest, in at least one {@code _timeseries} column. */
    public boolean hasRest() {
        return skips.isEmpty() == false;
    }
}
