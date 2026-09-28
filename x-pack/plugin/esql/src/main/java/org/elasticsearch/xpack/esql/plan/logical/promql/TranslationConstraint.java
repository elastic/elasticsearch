/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Labels required by name, optionally together with all current labels except a set of names.
 * This is a selection, not an inventory of storage projections. Combining open requirements needs only the
 * intersection of their exclusions; the scan always supplies one complete record.
 *
 * @param names concrete label reads
 * @param excluded null for a named-only selection; otherwise labels excluded from the open selection
 */
public record TranslationConstraint(Set<String> names, @Nullable Set<String> excluded) {
    public TranslationConstraint {
        names = unmodifiableSet(new LinkedHashSet<>(names));
        excluded = excluded == null ? null : unmodifiableSet(new LinkedHashSet<>(excluded));
    }

    /** Requests the complete current label record. */
    public static TranslationConstraint any() {
        return new TranslationConstraint(Set.of(), Set.of());
    }

    /** Requests only named labels; an empty collection requests no labels. */
    public static TranslationConstraint of(Collection<String> names) {
        return new TranslationConstraint(new LinkedHashSet<>(names), null);
    }

    public static TranslationConstraint of(String... names) {
        return of(List.of(names));
    }

    /** Combines requirements without creating additional physical record projections. */
    public static TranslationConstraint union(TranslationConstraint... constraints) {
        var names = new LinkedHashSet<String>();
        Set<String> excluded = null;
        for (var constraint : constraints) {
            names.addAll(constraint.names);
            if (constraint.isOpen()) {
                if (excluded == null) excluded = new LinkedHashSet<>(constraint.excluded);
                else excluded.retainAll(constraint.excluded);
            }
        }
        return new TranslationConstraint(names, excluded);
    }

    /** Selects labels in a but not in b, against the current record rather than the original stored series. */
    public static TranslationConstraint sub(TranslationConstraint a, TranslationConstraint b) {
        var names = new LinkedHashSet<String>();
        for (String name : a.names) {
            if (b.excludes(List.of(name))) names.add(name);
        }
        Set<String> excluded = null;
        if (a.isOpen()) {
            if (b.isOpen()) {
                for (String name : b.excluded) {
                    if (a.excluded.contains(name) == false && b.names.contains(name) == false) names.add(name);
                }
            } else {
                excluded = new LinkedHashSet<>(a.excluded);
                excluded.addAll(b.names);
            }
        }
        return new TranslationConstraint(names, excluded);
    }

    public boolean isOpen() {
        return excluded != null;
    }

    public boolean isEmpty() {
        return names.isEmpty() && isOpen() == false;
    }

    /** True if none of these labels can be supplied by this selection. */
    public boolean excludes(Collection<String> labels) {
        for (String name : labels) {
            if (names.contains(name) || isOpen() && excluded.contains(name) == false) return false;
        }
        return true;
    }
}
