/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * The tabular surface of a PromQL translation: the label columns a table has, as names. The same {@link Header} flows
 * down as the columns a subtree must expose and up as the columns it exposes. The plan carries the columns; a label is
 * found in a plan output by canonical name ({@link #findByName}) and a packing by its derived name ({@link #packedName}).
 */
public final class TranslationContext {

    private TranslationContext() {}

    // -- core --

    public record Header(Set<String> labels, Set<Set<String>> skips) {
        /** No columns: a scalar's header, and the identity of {@link #union}. */
        public static final Header EMPTY = new Header(Set.of(), Set.of());

        public Header {
            labels = Collections.unmodifiableSet(new LinkedHashSet<>(labels));
            var copy = new LinkedHashSet<Set<String>>();
            for (Set<String> skip : skips) {
                copy.add(Collections.unmodifiableSet(new LinkedHashSet<>(skip)));
            }
            skips = Collections.unmodifiableSet(copy);
        }

        public Header union(Header other) {
            var mergedLabels = new LinkedHashSet<>(labels);
            mergedLabels.addAll(other.labels);
            var mergedSkips = new LinkedHashSet<>(skips);
            mergedSkips.addAll(other.skips);
            return new Header(mergedLabels, mergedSkips);
        }

        /**
         * This header transposed below a node that drops {@code keys}: the dropped labels are no longer available as
         * columns, and every packed column must already exclude them to survive the regroup.
         */
        public Header difference(Collection<String> keys) {
            var remaining = new LinkedHashSet<>(labels);
            remaining.removeAll(keys);
            var widened = new LinkedHashSet<Set<String>>();
            for (Set<String> skip : skips) {
                var wider = new LinkedHashSet<>(skip);
                wider.addAll(keys);
                widened.add(wider);
            }
            return new Header(remaining, widened);
        }

        /**
         * The columns of this header that survive a node dropping {@code keys}: labels outside the set and packed
         * columns already excluding all of it. The upward counterpart of {@link #difference}.
         */
        public Header surviving(Collection<String> keys) {
            var remaining = new LinkedHashSet<>(labels);
            remaining.removeAll(keys);
            var covering = new LinkedHashSet<Set<String>>();
            for (Set<String> skip : skips) {
                if (skip.containsAll(keys)) {
                    covering.add(skip);
                }
            }
            return new Header(remaining, covering);
        }

        /** Only the labels among {@code names}; packed columns unchanged. Trims what a child exposes to what is required. */
        public Header retainLabels(Collection<String> names) {
            var retained = new LinkedHashSet<>(labels);
            retained.retainAll(names);
            return new Header(retained, skips);
        }

        /** The labels only; a finite table such as a join result cannot carry packed columns. */
        public Header finitePart() {
            return new Header(labels, Set.of());
        }

        /** The smallest skip set: the packed column fixing this table's grain; null when the table is unpacked. */
        public Set<String> finestSkip() {
            return skips.stream().min(Comparator.comparingInt(Set::size)).orElse(null);
        }

        public boolean hasPacked() {
            return skips.isEmpty() == false;
        }
    }

    /** Exactly these labels, each as its own column. */
    public static Header newFinite(Collection<String> names) {
        return new Header(new LinkedHashSet<>(names), Set.of());
    }

    /** Every runtime label except {@code skip}, as one packed column; an empty skip set is the full label space. */
    public static Header newOpen(Collection<String> skip) {
        return new Header(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    public static Header newOpen() {
        return newOpen(Set.of());
    }

    // -- helpers --

    static String packedName(Set<String> skip) {
        if (skip.isEmpty()) {
            return MetadataAttribute.TIMESERIES;
        }
        return MetadataAttribute.TIMESERIES + "$" + skip.stream().sorted().collect(Collectors.joining("$"));
    }

    static List<String> mapToNames(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(TranslationContext::toCanonicalName).distinct().toList();
    }

    static Attribute reference(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    static String toCanonicalName(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /**
     * The attribute resolution scope for a PromQL command that derives labels: each derived destination shadows any
     * stored label of the same name. Stored attributes whose canonical name collides with a destination are dropped and
     * the destinations added, so an enclosing {@code by(dst)}/{@code KEEP dst} binds unambiguously to the derived label
     * (a bare destination would otherwise collide with the stored label's bare passthrough alias).
     */
    public static List<Attribute> shadowedResolutionScope(List<Attribute> childrenOutput, List<Attribute> destinations) {
        Set<String> shadowed = new LinkedHashSet<>(mapToNames(destinations));
        List<Attribute> scope = new ArrayList<>(childrenOutput.size() + destinations.size());
        for (Attribute attribute : childrenOutput) {
            if (shadowed.contains(toCanonicalName(attribute)) == false) {
                scope.add(attribute);
            }
        }
        scope.addAll(destinations);
        return scope;
    }

    public static Attribute findByName(List<Attribute> attributes, String labelName) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (toCanonicalName(attribute).equals(labelName)) {
                if (attribute.name().equals(labelName) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
    }
}
