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
public record TranslationSchema(Set<String> labels, Set<Set<String>> skips) {
    /** No columns: a scalar's constraint, and the identity of {@link #newConstraintUnion}. */
    public static final TranslationSchema EMPTY = new TranslationSchema(Set.of(), Set.of());

    public TranslationSchema {
        labels = Collections.unmodifiableSet(new LinkedHashSet<>(labels));
        var copy = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : skips) {
            copy.add(Collections.unmodifiableSet(new LinkedHashSet<>(skip)));
        }
        skips = Collections.unmodifiableSet(copy);
    }

    /** Exactly these labels, promoted: each its own column. */
    public static TranslationSchema newConstraintWithPromoted(Collection<String> names) {
        return new TranslationSchema(new LinkedHashSet<>(names), Set.of());
    }

    /** The rest except {@code skip}, encoded in a {@code _timeseries} column; an empty skip set is the whole rest. */
    public static TranslationSchema newConstraintUnset(Collection<String> skip) {
        return new TranslationSchema(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** The whole rest: every label, encoded in the {@code _timeseries} column. */
    public static TranslationSchema newConstraintUnset() {
        return newConstraintUnset(Set.of());
    }

    /** Merge two constraints: promoted labels and exclusion sets of the rest combined. */
    public static TranslationSchema newConstraintUnion(TranslationSchema a, TranslationSchema b) {
        var mergedLabels = new LinkedHashSet<>(a.labels);
        mergedLabels.addAll(b.labels);
        var mergedSkips = new LinkedHashSet<>(a.skips);
        mergedSkips.addAll(b.skips);
        return new TranslationSchema(mergedLabels, mergedSkips);
    }

    /**
     * A constraint transposed below a node that drops {@code keys}: the dropped labels are no longer promoted, and every
     * {@code _timeseries} column of the rest must already exclude them to survive the regroup.
     */
    public static TranslationSchema newConstraintSub(TranslationSchema constraint, Collection<String> keys) {
        var remaining = new LinkedHashSet<>(constraint.labels);
        remaining.removeAll(keys);
        var widened = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : constraint.skips) {
            var wider = new LinkedHashSet<>(skip);
            wider.addAll(keys);
            widened.add(wider);
        }
        return new TranslationSchema(remaining, widened);
    }

    /**
     * The columns of a constraint that survive a node dropping {@code keys}: promoted labels outside the set and the
     * {@code _timeseries} columns of the rest already excluding all of it. The upward counterpart of {@link #newConstraintSub}.
     */
    public static TranslationSchema newConstraintIntersect(TranslationSchema constraint, Collection<String> keys) {
        var remaining = new LinkedHashSet<>(constraint.labels);
        remaining.removeAll(keys);
        var covering = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : constraint.skips) {
            if (skip.containsAll(keys)) {
                covering.add(skip);
            }
        }
        return new TranslationSchema(remaining, covering);
    }

    /**
     * A constraint without {@code keys}: the promoted labels outside the set, the rest unchanged. Where the series' one
     * {@code _timeseries} is edited in place ({@code TimeSeriesUnset}), a node dropping labels unsets them itself, so the
     * child keeps carrying its whole rest instead of one already excluding them ({@link #newConstraintSub}).
     */
    public static TranslationSchema newConstraintExclude(TranslationSchema constraint, Collection<String> keys) {
        var remaining = new LinkedHashSet<>(constraint.labels);
        remaining.removeAll(keys);
        return new TranslationSchema(remaining, constraint.skips);
    }

    /** Only the promoted labels among {@code names}; the rest unchanged. Trims what a child exposes to what is required. */
    public static TranslationSchema newConstraintProject(TranslationSchema constraint, Collection<String> names) {
        var retained = new LinkedHashSet<>(constraint.labels);
        retained.retainAll(names);
        return new TranslationSchema(retained, constraint.skips);
    }

    /** True when this constraint carries the rest, in at least one {@code _timeseries} column. */
    public boolean hasMetadata() {
        return skips.isEmpty() == false;
    }
}
