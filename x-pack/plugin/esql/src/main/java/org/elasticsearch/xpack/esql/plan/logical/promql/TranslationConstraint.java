/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * The labels a node requires of a table - what a parent requires its child to deliver (flowing down the PromQL tree), or
 * the keys an aggregate requires of its input - as set algebra over label names: {@link #of exactly these},
 * {@link #any every label}, their {@link #union}, one constraint {@link #sub less} another. It says what the header must
 * look like and nothing about how it is carried: a table resolves it to columns - the scan materialises a complement as a
 * packed column, a table with known labels answers it with those labels.
 *
 * @param names  labels required by name
 * @param allBut every label of the series except these, one set per complement
 */
public record TranslationConstraint(Set<String> names, Set<Set<String>> allBut) {

    public TranslationConstraint {
        names = unmodifiableSet(new LinkedHashSet<>(names));
        var complements = new LinkedHashSet<Set<String>>();
        for (var except : allBut) {
            complements.add(unmodifiableSet(new LinkedHashSet<>(except)));
        }
        allBut = unmodifiableSet(complements);
    }

    /** Every label of the series, whatever they turn out to be: what a node normally requires of its child. */
    public static TranslationConstraint any() {
        return new TranslationConstraint(Set.of(), Set.of(Set.of()));
    }

    /** Exactly these labels, by name; {@code of()} is no labels at all, what a bare aggregate or a scalar requires. */
    public static TranslationConstraint of(Collection<String> names) {
        return new TranslationConstraint(new LinkedHashSet<>(names), Set.of());
    }

    public static TranslationConstraint of(String... names) {
        return of(List.of(names));
    }

    /** All of the constraints together. */
    public static TranslationConstraint union(TranslationConstraint... constraints) {
        var names = new LinkedHashSet<String>();
        var allBut = new LinkedHashSet<Set<String>>();
        for (var constraint : constraints) {
            names.addAll(constraint.names);
            allBut.addAll(constraint.allBut);
        }
        return new TranslationConstraint(names, allBut);
    }

    /**
     * {@code a} less {@code b}: the labels {@code a} may deliver that {@code b} does not cover. A label {@code b} names
     * goes; a complement of {@code a} widens to exclude the named ones and, when {@code b} is open too, closes down to
     * the labels {@code b} leaves out. So {@code sub(union(any(), required), of(dropped))} is what a node dropping {@code dropped}
     * requires of its child, {@code sub(any(), of(x))} is every label but {@code x}, {@code sub(any(), any())} is
     * {@code of()} and {@code sub(any(), sub(any(), of(x)))} is {@code of(x)}.
     */
    public static TranslationConstraint sub(TranslationConstraint a, TranslationConstraint b) {
        // the labels every complement of b leaves out; null when b has none and so covers only the labels it names
        Set<String> leftOut = null;
        for (var except : b.allBut) {
            if (leftOut == null) {
                leftOut = new LinkedHashSet<>(except);
            } else {
                leftOut.retainAll(except);
            }
        }
        var names = new LinkedHashSet<String>();
        var allBut = new LinkedHashSet<Set<String>>();
        for (var name : a.names) {
            if (b.excludes(List.of(name))) {
                names.add(name);
            }
        }
        for (var except : a.allBut) {
            if (leftOut == null) {
                var widened = new LinkedHashSet<>(except);
                widened.addAll(b.names);
                allBut.add(widened);
            } else {
                for (var name : leftOut) {
                    if (b.names.contains(name) == false && except.contains(name) == false) {
                        names.add(name);
                    }
                }
            }
        }
        return new TranslationConstraint(names, allBut);
    }

    /** Whether the label set is not known at plan time: a complement is among the terms. */
    public boolean isOpen() {
        return allBut.isEmpty() == false;
    }

    public boolean isEmpty() {
        return names.isEmpty() && allBut.isEmpty();
    }

    /** True when no term can deliver a label in {@code labels}: none is required by name, every complement excludes them. */
    public boolean excludes(Collection<String> labels) {
        for (var name : names) {
            if (labels.contains(name)) {
                return false;
            }
        }
        for (var except : allBut) {
            if (except.containsAll(labels) == false) {
                return false;
            }
        }
        return true;
    }
}
