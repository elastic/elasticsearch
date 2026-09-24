/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The existing label requirements and available columns, shared by parent and child translations.
 * Labels required by name, optionally together with packed columns each excluding a set of names.
 * This is a selection, not an inventory of storage projections: combining requirements only combines
 * their label names and exclusion sets, without creating physical record projections.
 *
 * @param labels concrete label reads
 * @param skips exclusion sets, one per packed column
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

    /** Exactly these labels, each as its own column. */
    public static TranslationConstraint finite(Collection<String> names) {
        return new TranslationConstraint(new LinkedHashSet<>(names), Set.of());
    }

    /** Every runtime label except {@code skip}, as packed columns; an empty skip set is the full label space. */
    public static TranslationConstraint open(Collection<String> skip) {
        return new TranslationConstraint(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** Every runtime label **/
    public static TranslationConstraint open() {
        return open(Set.of());
    }

    /** Merge two constraints: labels and skip sets combined. */
    public static TranslationConstraint union(TranslationConstraint a, TranslationConstraint b) {
        var mergedLabels = new LinkedHashSet<>(a.labels);
        mergedLabels.addAll(b.labels);
        var mergedSkips = new LinkedHashSet<>(a.skips);
        mergedSkips.addAll(b.skips);
        return new TranslationConstraint(mergedLabels, mergedSkips);
    }

    /**
     * A constraint transposed below a node that drops {@code keys}: the dropped labels are no longer available as
     * columns, and every packed column must already exclude them to survive the regroup.
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
     * The columns of a constraint that survive a node dropping {@code keys}: labels outside the set and packed
     * columns already excluding all of it. The upward counterpart of {@link #subtract}.
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

    /** Only the labels among {@code names}; packed columns unchanged. Trims what a child exposes to what is required. */
    public static TranslationConstraint project(TranslationConstraint constraint, Collection<String> names) {
        var retained = new LinkedHashSet<>(constraint.labels);
        retained.retainAll(names);
        return new TranslationConstraint(retained, constraint.skips);
    }

    /** The smallest skip set: the packed column fixing this table's grain; null when the table is unpacked. */
    public Set<String> finestSkip() {
        return skips.stream().min(Comparator.comparingInt(Set::size)).orElse(null);
    }

    /** True when this constraint carries at least one packed column. */
    public boolean isOpen() {
        return skips.isEmpty() == false;
    }
}
