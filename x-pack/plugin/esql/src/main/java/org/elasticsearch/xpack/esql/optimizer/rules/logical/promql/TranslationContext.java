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
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramFunctionCall;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Collections.unmodifiableSortedSet;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * The tabular surface of a PromQL translation: the label columns of a table, as one {@link Header} that flows down as
 * the columns a subtree must expose and up as the columns it does expose. Going down the header is a requirement -
 * names only; coming up each column is bound to the output attribute of the plan producing it, so a child's output
 * header is directly the parent's input. {@link #bind(Header, Header)} does the binding at a node boundary, resolving every
 * required column to a plan attribute or a null-fill.
 */
public final class TranslationContext {

    private TranslationContext() {}

    // -- core --

    /**
     * The label columns of a table. A regular column is one label; a packed column carries every runtime label except
     * its exclusions, so an empty exclusion set is the full label space. Packed columns iterate by increasing exclusions
     * (the first packs the most labels and so fixes the table's grain), so consumers turning a header into grouping keys
     * need no further ordering.
     * <p>
     * {@code columnExpr} binds columns to the plan attributes producing them, keyed by canonical column name: a regular
     * column by its label, a packed one by {@link #mapOpen}. A requirement binds nothing; a
     * translated table's header is fully bound ({@link #isBound()}). Headers compose through the static algebra below
     * ({@link #finite}, {@link #open}, {@link #union}, {@link #sub}, {@link #filter}, {@link #select}, {@link #bind}), which reads as the set expression
     * it computes and keeps the bindings of the columns that survive.
     */
    public record Header(Set<String> finiteColumns, Set<Set<String>> openColumns, Map<String, Attribute> columnExpr) {

        /** No columns: a scalar's header, and the identity of {@link #union}. */
        public static final Header PassThrough = new Header();

        /**
         * Fewer exclusions lead. Ties break on the canonical name, which is unique per exclusion set, so the order is
         * total and distinct packed columns of equal size never collide in the sorted set.
         */
        private static final Comparator<Set<String>> BY_EXCLUSIONS = Comparator.<Set<String>>comparingInt(Set::size)
            .thenComparing(TranslationContext::mapOpen);

        public Header() {
            this(Set.of(), Set.of());
        }

        /** A requirement: these columns, none bound. */
        public Header(Set<String> finiteColumns, Set<Set<String>> openColumns) {
            this(finiteColumns, openColumns, Map.of());
        }

        public Header {
            finiteColumns = unmodifiableSet(new LinkedHashSet<>(finiteColumns));
            var sorted = new TreeSet<>(BY_EXCLUSIONS);
            openColumns.forEach(exclusions -> sorted.add(unmodifiableSet(new LinkedHashSet<>(exclusions))));
            openColumns = unmodifiableSortedSet(sorted);
            // Only the columns of this header stay bound: narrowing a header (sub, filter, select) drops the
            // bindings of the columns it removes along with them.
            var bound = new LinkedHashMap<>(columnExpr);
            bound.keySet().retainAll(names(finiteColumns, openColumns));
            columnExpr = unmodifiableMap(bound);
        }

        /** The canonical names of every column, regular then packed. */
        private static Set<String> names(Set<String> finiteColumns, Set<Set<String>> openColumns) {
            var names = new LinkedHashSet<>(finiteColumns);
            openColumns.forEach(exclusions -> names.add(mapOpen(exclusions)));
            return names;
        }

        /** The attribute producing a regular column, or null when the table lacks it or the header is a requirement. */
        public Attribute getExpr(String name) {
            return columnExpr.get(name);
        }

        /** The attribute producing a packed column, or null when the table lacks it or the header is a requirement. */
        public Attribute getExpr(Set<String> exclusions) {
            return columnExpr.get(mapOpen(exclusions));
        }

        public boolean isEmpty() {
            return finiteColumns.isEmpty() && openColumns.isEmpty();
        }

        /** True when the header has at least one packed column. */
        public boolean isOpen() {
            return openColumns.isEmpty() == false;
        }

        /** True when every column is bound to an attribute: the header of a translated table rather than a requirement. */
        public boolean isBound() {
            return columnExpr.keySet().containsAll(names(finiteColumns, openColumns));
        }

        /** The attributes of a bound header as grouping keys: packed columns by increasing exclusions, then regular columns. */
        public List<Attribute> expressions() {
            assert isBound() : "invariant: only a bound header has key attributes: " + this;
            var attributes = new ArrayList<Attribute>();
            openColumns.forEach(exclusions -> attributes.add(getExpr(exclusions)));
            finiteColumns.forEach(name -> attributes.add(getExpr(name)));
            return attributes;
        }

        /**
         * The columns of this bound header {@code plan} does not produce yet, as the null-valued definitions it must
         * add: the columns {@link TranslationContext#bind(Header, Header)} could not resolve against the input, or any attribute minted for a column the
         * table lacks.
         */
        public List<Alias> nullFills(LogicalPlan plan) {
            var outputs = plan.outputSet();
            return expressions().stream()
                .filter(expr -> outputs.contains(expr) == false)
                .map(TranslationContext::emitNullExpression)
                .toList();
        }

        /** Every binding replaced through {@code rebind}: the same columns over a plan whose attributes changed. */
        public Header map(UnaryOperator<Attribute> rebind) {
            var rebound = new LinkedHashMap<>(columnExpr);
            rebound.replaceAll((name, expr) -> rebind.apply(expr));
            return new Header(finiteColumns, openColumns, rebound);
        }
    }

    // -- header algebra --
    // Constructors lift names into headers; every operator is then Header x Header, so label sets never travel as
    // bare collections: `union(sub(req, dropped), open(dropped))` reads as the set expression it computes.

    /** The classic-histogram bucket bound as a header, for the histogram functions that consume it. */
    static final Header _LE = finite(List.of(HistogramFunctionCall.LE_LABEL));

    /** Exactly these labels, each as its own column. */
    public static Header finite(Collection<String> names) {
        return new Header(new LinkedHashSet<>(names), Set.of());
    }

    /** Every runtime label except {@code exclusions}, as one packed column; no exclusions is the full label space. */
    public static Header open(Collection<String> exclusions) {
        return new Header(Set.of(), Set.of(new LinkedHashSet<>(exclusions)));
    }

    /** Every runtime label except the regular columns of {@code exclusions}, as one packed column. */
    public static Header open(Header exclusions) {
        return open(exclusions.finiteColumns());
    }

    /** Every runtime label **/
    public static Header open() {
        return open(Set.of());
    }

    /** The headers merged: regular and packed columns combined; a column bound in several keeps its first binding. */
    public static Header union(Header... headers) {
        var finiteColumns = new LinkedHashSet<String>();
        var openColumns = new LinkedHashSet<Set<String>>();
        var columnExpr = new LinkedHashMap<String, Attribute>();
        for (var header : headers) {
            finiteColumns.addAll(header.finiteColumns());
            openColumns.addAll(header.openColumns());
            header.columnExpr().forEach(columnExpr::putIfAbsent);
        }
        return new Header(finiteColumns, openColumns, columnExpr);
    }

    /**
     * The header transposed below a node that drops the columns of {@code dropped}: they are no longer available as
     * columns, and every packed column must already exclude them to survive the regroup. A widened packed column is a
     * new column nothing produces yet, so it comes back unbound.
     */
    public static Header sub(Header header, Header dropped) {
        var remaining = new LinkedHashSet<>(header.finiteColumns());
        remaining.removeAll(dropped.finiteColumns());
        var widened = new LinkedHashSet<Set<String>>();
        for (var exclusions : header.openColumns()) {
            var wider = new LinkedHashSet<>(exclusions);
            wider.addAll(dropped.finiteColumns());
            widened.add(wider);
        }
        return new Header(remaining, widened, header.columnExpr());
    }

    /** Only the regular columns also in {@code kept}; packed columns unchanged. Trims a header to what a table can produce. */
    public static Header filter(Header header, Header kept) {
        var retained = new LinkedHashSet<>(header.finiteColumns());
        retained.retainAll(kept.finiteColumns());
        return new Header(retained, header.openColumns(), header.columnExpr());
    }

    /**
     * The columns of {@code header} selected by a node that drops the columns of {@code dropped}: regular columns outside
     * the set and packed columns already excluding all of it, bindings kept. The upward counterpart of {@link #sub}.
     */
    public static Header select(Header header, Header dropped) {
        var remaining = new LinkedHashSet<>(header.finiteColumns());
        remaining.removeAll(dropped.finiteColumns());
        var covering = new LinkedHashSet<Set<String>>();
        for (var exclusions : header.openColumns()) {
            if (exclusions.containsAll(dropped.finiteColumns())) {
                covering.add(exclusions);
            }
        }
        return new Header(remaining, covering, header.columnExpr());
    }

    /**
     * The requirement {@code header} bound to the columns of {@code input}: every packed column to the attribute the
     * input has for it, every regular column to the input's attribute or, where the input lacks the column, to a fresh
     * reference a plan must define as null ({@link Header#nullFills}). A declared label the input lacks is absent from
     * every series, so it groups under null like in Prometheus.
     */
    public static Header bind(Header header, Header input) {
        var bound = new LinkedHashMap<String, Attribute>();
        for (Set<String> exclusions : header.openColumns()) {
            Attribute expr = input.getExpr(exclusions);
            assert expr != null : "invariant: packed column " + exclusions + " must be produced by the input " + input;
            bound.put(mapOpen(exclusions), expr);
        }
        for (String name : header.finiteColumns()) {
            Attribute expr = input.getExpr(name);
            bound.put(name, expr != null ? expr : mapToRef(name));
        }
        return new Header(header.finiteColumns(), header.openColumns(), bound);
    }

    /** {@code header} with a regular column bound (or rebound) to {@code expr}, added if absent. */
    public static Header bind(Header header, String name, Attribute expr) {
        var finiteColumns = new LinkedHashSet<>(header.finiteColumns());
        finiteColumns.add(name);
        var rebound = new LinkedHashMap<>(header.columnExpr());
        rebound.put(name, expr);
        return new Header(finiteColumns, header.openColumns(), rebound);
    }

    /** {@code header} with a packed column bound (or rebound) to {@code expr}, added if absent. */
    public static Header bind(Header header, Set<String> exclusions, Attribute expr) {
        var openColumns = new LinkedHashSet<>(header.openColumns());
        openColumns.add(exclusions);
        var rebound = new LinkedHashMap<>(header.columnExpr());
        rebound.put(mapOpen(exclusions), expr);
        return new Header(header.finiteColumns(), openColumns, rebound);
    }

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its defined columns. A
     * {@link Header} flows down as the columns a subtree must expose; what flows up is this table itself - the plan
     * plus its header, every column bound to the plan attribute producing it. Every AST node translates to one and the
     * stitching operations (joins, unions, aggregates, the command coda) compose them by their declared columns.
     * Mid-descent the value is a (possibly not yet materialized) expression parents compose into larger expressions;
     * a finished table's value is a defined column ({@link #valueColumn()}).
     */
    record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* The regular and packed columns this subtree exposes, each bound to the plan output. */
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

        IntermediateResult {
            assert header.isBound() : "invariant: a translated table binds every column of its header: " + header;
            assert header.columnExpr().values().stream().allMatch(plan.outputSet()::contains)
                : "invariant: column expressions must belong to the output of " + plan;
        }

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

        IntermediateResult(LogicalPlan plan, Expression value, Attribute step) {
            this(plan, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Attribute step, Expression filter, Kind kind) {
            this(plan, Header.PassThrough, value, step, filter, kind);
        }

        /** This table rebuilt around a new plan and value, keeping its header and other properties. */
        IntermediateResult with(LogicalPlan plan, Expression value) {
            return with(plan, header, value);
        }

        /** This table rebuilt around a new plan, header and value, keeping its other properties. */
        IntermediateResult with(LogicalPlan plan, Header header, Expression value) {
            return new IntermediateResult(plan, header, value, step, pendingFilter, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        Attribute valueColumn() {
            return (Attribute) value;
        }

        /** The attribute producing a regular column in this table's plan, or null when the table lacks it. */
        Attribute getExpr(String name) {
            return header.getExpr(name);
        }

        /** The attribute producing a packed column in this table's plan, or null when the table lacks it. */
        Attribute getExpr(Set<String> exclusions) {
            return header.getExpr(exclusions);
        }
    }

    // -- helpers --

    static String mapOpen() {
        return mapOpen(Set.of());
    }

    static String mapOpen(Set<String> exclusions) {
        var s = String.join(SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR, new TreeSet<>(exclusions));
        return MetadataAttribute.TIMESERIES + (s.isEmpty() ? s : SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR + s);
    }

    static List<String> mapFinite(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(TranslationContext::mapFinite).distinct().toList();
    }

    static String mapFinite(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    static Attribute mapToRef(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
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

    public static Attribute find(List<Attribute> attributes, Set<String> excluded) {
        for (var attr : attributes) {
            if (attr instanceof TimeSeriesMetadataAttribute ma) {
                if (ma.excludedFields().equals(excluded)) {
                    return ma;
                }
            }
        }

        return null;
    }
}
