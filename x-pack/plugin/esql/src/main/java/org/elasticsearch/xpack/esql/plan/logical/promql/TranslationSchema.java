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
 * column, and optionally the time-series metadata - every other label, encoded in a {@code _timeseries} column -
 * once per exclusion set. This is strictly a top-down propagation mechanism: a parent passes a requirement down and then
 * reads whatever it needs off the child plan's output.
 * It is a selection, not an inventory of storage projections: combining requirements only combines
 * their label names and exclusion sets, without creating physical record projections. The metadata may overlap the
 * promoted names.
 *
 * @param labels promoted label reads
 * @param skips exclusion sets of the metadata, one per {@code _timeseries} column
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

    /** The metadata with {@code skip} unset, encoded in a {@code _timeseries} column; with nothing unset, the whole metadata. */
    public static TranslationSchema newConstraintUnset(Collection<String> skip) {
        return new TranslationSchema(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** The whole metadata, nothing unset: every label, encoded in the {@code _timeseries} column. */
    public static TranslationSchema newConstraintUnset() {
        return newConstraintUnset(Set.of());
    }

    /** Merge two constraints: promoted labels and metadata exclusion sets combined. */
    public static TranslationSchema newConstraintUnion(TranslationSchema a, TranslationSchema b) {
        var mergedLabels = new LinkedHashSet<>(a.labels);
        mergedLabels.addAll(b.labels);
        var mergedSkips = new LinkedHashSet<>(a.skips);
        mergedSkips.addAll(b.skips);
        return new TranslationSchema(mergedLabels, mergedSkips);
    }

    /**
     * A constraint transposed below a node that drops {@code keys}: the dropped labels are no longer promoted, and every
     * metadata {@code _timeseries} column must already exclude them to survive the regroup.
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
     * metadata {@code _timeseries} columns already excluding all of it. The upward counterpart of {@link #newConstraintSub}.
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

    /** Only the promoted labels among {@code names}; the metadata unchanged. Trims what a child exposes to what is required. */
    public static TranslationSchema newConstraintProject(TranslationSchema constraint, Collection<String> names) {
        var retained = new LinkedHashSet<>(constraint.labels);
        retained.retainAll(names);
        return new TranslationSchema(retained, constraint.skips);
    }

    /** True when this constraint carries metadata, in at least one {@code _timeseries} column. */
    public boolean hasMetadata() {
        return skips.isEmpty() == false;
    }
}
