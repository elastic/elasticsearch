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
public record TranslationSchema(Set<String> labels, Set<Set<String>> skips) {
    /** No columns: a scalar's schema, and the identity of {@link #union}. */
    public static final TranslationSchema EMPTY = new TranslationSchema(Set.of(), Set.of());

    public TranslationSchema {
        labels = Collections.unmodifiableSet(new LinkedHashSet<>(labels));
        var copy = new LinkedHashSet<Set<String>>();
        for (Set<String> skip : skips) {
            copy.add(Collections.unmodifiableSet(new LinkedHashSet<>(skip)));
        }
        skips = Collections.unmodifiableSet(copy);
    }

    /** Exactly these labels, each as its own column. */
    public static TranslationSchema finite(Collection<String> names) {
        return new TranslationSchema(new LinkedHashSet<>(names), Set.of());
    }

    /** Every runtime label except {@code skip}, as packed columns; an empty skip set is the full label space. */
    public static TranslationSchema open(Collection<String> skip) {
        return new TranslationSchema(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** Every runtime label **/
    public static TranslationSchema open() {
        return open(Set.of());
    }

    /** Merge two constraints: labels and skip sets combined. */
    public static TranslationSchema union(TranslationSchema a, TranslationSchema b) {
        var mergedLabels = new LinkedHashSet<>(a.labels);
        mergedLabels.addAll(b.labels);
        var mergedSkips = new LinkedHashSet<>(a.skips);
        mergedSkips.addAll(b.skips);
        return new TranslationSchema(mergedLabels, mergedSkips);
    }

    /**
     * A constraint transposed below a node that drops {@code keys}: the dropped labels are no longer available as
     * columns, and every packed column must already exclude them to survive the regroup.
     */
    public static TranslationSchema subtract(TranslationSchema constraint, Collection<String> keys) {
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
     * The columns of a constraint that survive a node dropping {@code keys}: labels outside the set and packed
     * columns already excluding all of it. The upward counterpart of {@link #subtract}.
     */
    public static TranslationSchema intersect(TranslationSchema constraint, Collection<String> keys) {
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

    /** Only the labels among {@code names}; packed columns unchanged. Trims what a child exposes to what is required. */
    public static TranslationSchema project(TranslationSchema constraint, Collection<String> names) {
        var retained = new LinkedHashSet<>(constraint.labels);
        retained.retainAll(names);
        return new TranslationSchema(retained, constraint.skips);
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
