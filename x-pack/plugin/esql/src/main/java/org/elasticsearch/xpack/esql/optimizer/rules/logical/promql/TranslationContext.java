/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * The tabular surface of a PromQL translation: the label columns a table has, as names. The same {@link Header} flows
 * down as the columns a subtree must expose and up as the columns it exposes. The plan carries the columns; a label is
 * found in a plan output by canonical name ({@link #find}) and a packing by its derived name ({@link #mapOpen}).
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

        /** Merge two headers: labels and skip sets combined. */
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
        public Header subtract(Collection<String> keys) {
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
         * columns already excluding all of it. The upward counterpart of {@link #subtract}.
         */
        public Header intersect(Collection<String> keys) {
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
        public Header project(Collection<String> names) {
            var retained = new LinkedHashSet<>(labels);
            retained.retainAll(names);
            return new Header(retained, skips);
        }

        /** The smallest skip set: the packed column fixing this table's grain; null when the table is unpacked. */
        public Set<String> finestSkip() {
            return skips.stream().min(Comparator.comparingInt(Set::size)).orElse(null);
        }

        /** True when this header carries at least one packed column. */
        public boolean isOpen() {
            return skips.isEmpty() == false;
        }
    }

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its defined columns. The
     * {@link Header} names the label columns and the plan carries them; value and step are the two columns every table
     * has. Every AST node translates to one and the stitching operations (joins, unions, regroups, the command coda)
     * compose them by their declared columns. Mid-descent the value is a (possibly not yet materialized) expression
     * parents compose into larger expressions; a finished table's value is a defined column ({@link #valueColumn()}).
     */
    record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* The label columns this subtree exposes; the plan carries them under their canonical or derived names. */
        Header header,
        /* This node's numeric value: an expression mid-descent, a defined column once aggregated. */
        Expression value,
        /* The step column. */
        Attribute step,
        /* Label matcher predicate; flows up until pushed to the relation or folded into an aggregate filter. */
        Expression pendingFilter,
        /* The translator tracks what it built instead of inspecting the plan. */
        Kind kind
    ) {
        /** The lifecycle of an intermediate result. A constant is always a finished (aggregation-free) local relation. */
        enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true);

            final boolean constant;
            final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        IntermediateResult(LogicalPlan plan, Header header, Expression value, Attribute step) {
            this(plan, header, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Header header, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, header, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan, header and value, keeping its other properties. */
        IntermediateResult with(LogicalPlan plan, Header header, Expression value) {
            return new IntermediateResult(plan, header, value, step, pendingFilter, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        Attribute valueColumn() {
            return (Attribute) value;
        }

        /** The attribute carrying a label in this table's plan, or null when the table lacks it. */
        Attribute label(String name) {
            return find(plan.output(), name);
        }

        /** The attribute carrying a packing in this table's plan, or null when the table lacks it. */
        Attribute packed(Set<String> skip) {
            return find(plan.output(), mapOpen(skip));
        }
    }

    /** Exactly these labels, each as its own column. */
    public static Header finite(Collection<String> names) {
        return new Header(new LinkedHashSet<>(names), Set.of());
    }

    /** Every runtime label except {@code skip}, as one packed column; an empty skip set is the full label space. */
    public static Header open(Collection<String> skip) {
        return new Header(Set.of(), Set.of(new LinkedHashSet<>(skip)));
    }

    /** Every runtime label **/
    public static Header open() {
        return open(Set.of());
    }

    // -- helpers --

    static String mapOpen() {
        return mapOpen(Set.of());
    }

    static String mapOpen(Set<String> skip) {
        return MetadataAttribute.TIMESERIES + (skip.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(skip)));
    }

    static List<String> mapFinite(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(TranslationContext::mapFinite).distinct().toList();
    }

    static String mapFinite(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    static Attribute mapToRef(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /** The skip sets of a header ordered finest first: the grain-fixing packing leads, coarser variants follow. */
    static List<Set<String>> finestFirst(Set<Set<String>> skips) {
        return skips.stream().sorted(Comparator.comparingInt(Set::size)).toList();
    }

    /** A null-valued column under the attribute's own name and id, typed like the attribute (keyword when unresolved). */
    static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    public static Attribute find(List<Attribute> attributes, String label) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (mapFinite(attribute).equals(label)) {
                if (attribute.name().equals(label) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
    }
}
