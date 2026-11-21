/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The PromQL translation threads the labels of a series through the plan as one keyword label container, {@code _timeseries},
 * the JSON object of the series' dimensions, and reads a label out of it with {@link TimeSeriesMetadataPath}, which parses the
 * container row by row. This rule replaces such a read with the value it reads, when that value is in the plan already: a
 * dimension field of the source, a value a {@link TimeSeriesMetadataSet} wrote, or nothing. A read it cannot replace stays and
 * stays correct, so the rule is an optimization only.
 * <p>
 * It follows what engines do with a read of a field of a struct or a JSON column:
 * <ul>
 *     <li>A read of what a function just built is folded, as Spark's {@code SimplifyExtractValueOps} folds a field of a struct
 *     and LLVM folds {@code extractvalue} of {@code insertvalue}: a read of a written label is the written value, of a label a
 *     {@link TimeSeriesMetadataFilter} leaves out is null, of any other label is a read of the container below.</li>
 *     <li>A read of an attribute follows the attribute to its definition, which a {@code NameId} identifies alone, as a load is
 *     forwarded to the store it reads. Only a read that ends at a value the plan has is replaced.</li>
 *     <li>A read of the container the time-series aggregate groups by is the source's dimension field, which the aggregate
 *     groups by too: the series determines its dimensions, so the groups stay, and the aggregate reads a dimension once per
 *     series, whole, as it reads a label the query groups by. The fields of a column of semi-structured data are read the
 *     same way in Sinew and JSON Tiles. A label the container leaves out, or that no field holds, is null. The container
 *     is loaded from {@code _source} and the field from its doc values, so a label of several values reads sorted and
 *     without repeats, and a normalized one normalized, as a label grouped by {@code by(...)} reads; {@code PromQL} labels
 *     have one value and no normalizer, so they read the same. Only keyword dimensions are read: the source text of any
 *     other type, such as {@code 1.0} coerced into a {@code long}, is not in its doc values.</li>
 *     <li>The value gets a temporary name where it is defined and goes up to the read as Trino's dereference pushdown carries
 *     a field: a {@link Project} on the way projects it, a node that keeps its input's output passes it, and either side of a
 *     {@link HashJoin} passes it, the temporary name keeping it apart from the other side's labels. A plain {@link Aggregate}
 *     stops it, as a key of several values would split the aggregate's rows; so does any other node.</li>
 *     <li>A read of a {@link UnionAll}'s container is a read of each branch's container at the same position, as a projection
 *     commutes with a union (the commuting conversions of query normalisation, Spark's projection pushdown through a union):
 *     when every branch has the value, the union outputs it as one more column, which its branches give under its name.</li>
 * </ul>
 * A container that is still used as a whole - a key of an aggregate, a join or a pack, or the output - stays as it is. Where
 * nothing uses it anymore, column pruning removes it.
 * <p>
 * Only labels named by literals are followed: a label named by anything else may be any label, as LLVM folds only constant
 * indices. A written literal is its value unless it is empty, which removes the label, or holds several values, which may hold
 * an empty one. A written attribute is its value: the translation writes a label column, which holds a label's values as the
 * translation reads them from a dimension field, or a computed label, whose empty value it turns into null.
 */
public final class ScalarizeTimeSeriesMetadataPaths extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan.anyMatch(node -> node.expressions().stream().anyMatch(e -> e.anyMatch(TimeSeriesMetadataPath.class::isInstance)))) {
            return new Run().apply(plan);
        }
        return plan;
    }

    /**
     * One run: first, from the root down and changing nothing, each read is followed to its value; then, from the leaves up,
     * the plan is rebuilt with the reads replaced and the values carried up to them. The nodes are the ones of the plan the
     * run started with, by identity.
     */
    private static class Run {
        /** The value of each read that has one, by the read. */
        private final Map<Expression, Expression> values = new IdentityHashMap<>();
        /** The dimension fields each time-series aggregate groups by in addition. */
        private final Map<LogicalPlan, List<FieldAttribute>> keys = new IdentityHashMap<>();
        /** The temporary names to give values right above the node whose output holds them, by node, then by value. */
        private final Map<LogicalPlan, Map<Expression, Alias>> names = new IdentityHashMap<>();
        /** The values each projection projects, or each join adds of its right side, in addition. */
        private final Map<LogicalPlan, List<Attribute>> carried = new IdentityHashMap<>();
        /** The columns each union outputs in addition, by the position and label they read. */
        private final Map<LogicalPlan, Map<String, Attribute>> columns = new IdentityHashMap<>();
        /** The value each branch of a union gives each of the union's added columns, in their order. */
        private final Map<LogicalPlan, List<Expression>> branches = new IdentityHashMap<>();

        /** The nodes crossed while following the current read, from the read down; committed only when the read has a value. */
        private List<LogicalPlan> crossed;
        /** The time-series aggregate the current read groups by a field, null when it reads none. */
        private TimeSeriesAggregate aggregate;
        /** The node whose output holds the value of the current read. */
        private LogicalPlan origin;

        LogicalPlan apply(LogicalPlan plan) {
            plan.forEachDown(this::follow);
            if (values.isEmpty()) {
                return plan;
            }
            // The values carried up show in the output of the nodes they cross; a projection keeps the plan's own.
            LogicalPlan rewritten = rewrite(plan);
            return rewritten.output().equals(plan.output()) ? rewritten : new Project(plan.source(), rewritten, plan.output());
        }

        /** Follows each read of {@code node} to its value. */
        private void follow(LogicalPlan node) {
            for (Expression expression : node.expressions()) {
                expression.forEachDown(TimeSeriesMetadataPath.class, read -> {
                    String label = name(read.labels().getFirst());
                    if (label == null) {
                        return;
                    }
                    crossed = new ArrayList<>();
                    aggregate = null;
                    origin = null;
                    Expression value = valueOf(read.children().getFirst(), label, node);
                    if (value != null) {
                        commit(read, value);
                    }
                });
            }
        }

        /**
         * The value of label {@code label} of {@code container}, an expression of {@code scope}; null when it has none. An
         * {@link Eval} sees the aliases it defines, so a container one of them defines is followed there.
         */
        private Expression valueOf(Expression container, String label, LogicalPlan scope) {
            switch (container) {
                case TimeSeriesMetadataSet set -> {
                    String written = name(set.label());
                    if (written == null) {
                        // a label named by anything but a literal may be this one
                        return null;
                    }
                    if (label.equals(written) == false) {
                        return valueOf(set.children().getFirst(), label, scope);
                    }
                    Expression value = set.value();
                    if (value instanceof TimeSeriesMetadataPath read) {
                        // a label copied from another container is that container's label
                        String copied = name(read.labels().getFirst());
                        return copied == null ? null : valueOf(read.children().getFirst(), copied, scope);
                    }
                    if (value instanceof Literal literal) {
                        // the write leaves an empty value out; several values may hold one
                        if (literal.value() == null || (literal.value() instanceof BytesRef bytes && bytes.length == 0)) {
                            return nothing(container);
                        }
                        return literal.value() instanceof BytesRef ? literal : null;
                    }
                    // a column, or the text of a column that isn't a string, which is never empty, is what the write keeps
                    Expression column = value instanceof ToString text && isNeverEmptyText(text.field().dataType()) ? text.field() : value;
                    if (column instanceof Attribute attribute
                        && scope.children().size() == 1
                        && scope.children().getFirst().outputSet().contains(attribute)) {
                        origin = scope.children().getFirst();
                        return value;
                    }
                    return null;
                }
                case TimeSeriesMetadataFilter filter -> {
                    if (filter.labels().stream().anyMatch(l -> name(l) == null)) {
                        return null;
                    }
                    boolean named = filter.labels().stream().anyMatch(l -> label.equals(name(l)));
                    return named == filter.exclude() ? nothing(container) : valueOf(filter.children().getFirst(), label, scope);
                }
                case Literal literal when literal.value() instanceof BytesRef bytes && bytes.utf8ToString().equals("{}") -> {
                    return nothing(container);
                }
                case Attribute attribute -> {
                    if (scope instanceof Eval eval) {
                        for (Alias field : eval.fields()) {
                            if (field.id().equals(attribute.id())) {
                                return valueOf(field.child(), label, eval);
                            }
                        }
                    }
                    for (LogicalPlan input : scope.children()) {
                        if (input.outputSet().contains(attribute)) {
                            return definition(input, attribute, label);
                        }
                    }
                    return null;
                }
                default -> {
                    return null;
                }
            }
        }

        /** The value of label {@code label} of container {@code attribute}, which {@code node} outputs. */
        private Expression definition(LogicalPlan node, Attribute attribute, String label) {
            crossed.add(node);
            switch (node) {
                case TimeSeriesAggregate timeSeriesAggregate -> {
                    for (Expression grouping : timeSeriesAggregate.groupings()) {
                        if (grouping instanceof Alias alias
                            && alias.id().equals(attribute.id())
                            && alias.child() instanceof TimeSeriesWithout without) {
                            return dimension(timeSeriesAggregate, without, label, alias);
                        }
                    }
                    return null;
                }
                case Aggregate ignored -> {
                    return null;
                }
                case Eval eval -> {
                    crossed.removeLast();
                    for (Alias field : eval.fields()) {
                        if (field.id().equals(attribute.id())) {
                            return valueOf(field.child(), label, eval);
                        }
                    }
                    return definition(eval.child(), attribute, label);
                }
                case Project project -> {
                    for (NamedExpression projection : project.projections()) {
                        if (projection.id().equals(attribute.id())) {
                            Expression projected = projection instanceof Alias alias ? alias.child() : projection;
                            return valueOf(projected, label, project);
                        }
                    }
                    return null;
                }
                case UnionAll union -> {
                    crossed.removeLast();
                    for (int position = 0; position < union.output().size(); position++) {
                        if (union.output().get(position).id().equals(attribute.id())) {
                            return column(union, position, label);
                        }
                    }
                    return null;
                }
                case HashJoin join -> {
                    if (join.left().outputSet().contains(attribute)) {
                        crossed.removeLast();
                        return definition(join.left(), attribute, label);
                    }
                    // the join adds what it names of its right side
                    return join.right().outputSet().contains(attribute) ? definition(join.right(), attribute, label) : null;
                }
                case UnaryPlan unary when unary.outputSet().containsAll(unary.child().outputSet()) -> {
                    crossed.removeLast();
                    return definition(unary.child(), attribute, label);
                }
                default -> {
                    return null;
                }
            }
        }

        /**
         * The value of label {@code label} of the container {@code aggregate} groups by: the source's keyword dimension field of
         * that label, null when the container leaves the label out or no field holds it.
         */
        private Expression dimension(TimeSeriesAggregate timeSeriesAggregate, TimeSeriesWithout without, String label, Alias container) {
            crossed.removeLast();
            for (Expression excluded : without.children()) {
                if (excluded instanceof Attribute field && label.equals(PromqlLabels.labelName(field))) {
                    return nothing(container);
                }
            }
            FieldAttribute dimension = null;
            for (Attribute field : timeSeriesAggregate.child().output()) {
                if (label.equals(PromqlLabels.labelName(field))) {
                    if (dimension != null || field instanceof FieldAttribute == false) {
                        return null;
                    }
                    dimension = (FieldAttribute) field;
                }
            }
            if (dimension == null) {
                boolean held = timeSeriesAggregate.child()
                    .collect(EsRelation.class)
                    .stream()
                    .anyMatch(relation -> relation.output().stream().anyMatch(field -> label.equals(PromqlLabels.labelName(field))));
                return held ? null : nothing(container);
            }
            if (dimension.isDimension() == false || dimension.dataType() != DataType.KEYWORD) {
                return null;
            }
            aggregate = timeSeriesAggregate;
            origin = timeSeriesAggregate;
            return dimension;
        }

        /**
         * The value of label {@code label} of the container {@code union} outputs at {@code position}: a column the union
         * outputs in addition, which each branch gives the value of the label of its container at that position. A projection
         * of a union is the union of the projections of its branches; a branch that has no value leaves the union without one.
         */
        private Expression column(UnionAll union, int position, String label) {
            String key = position + ":" + label;
            Attribute known = columns.getOrDefault(union, Map.of()).get(key);
            if (known == null) {
                List<LogicalPlan> above = crossed;
                List<Expression> found = new ArrayList<>();
                List<List<LogicalPlan>> paths = new ArrayList<>();
                List<LogicalPlan> origins = new ArrayList<>();
                List<TimeSeriesAggregate> aggregates = new ArrayList<>();
                for (LogicalPlan branch : union.children()) {
                    crossed = new ArrayList<>();
                    aggregate = null;
                    origin = null;
                    Expression value = definition(branch, branch.output().get(position), label);
                    if (value == null) {
                        crossed = above;
                        return null;
                    }
                    found.add(value);
                    paths.add(crossed);
                    origins.add(origin);
                    aggregates.add(aggregate);
                }
                for (int i = 0; i < found.size(); i++) {
                    crossed = paths.get(i);
                    origin = origins.get(i);
                    aggregate = aggregates.get(i);
                    Expression value = found.get(i) instanceof Literal ? found.get(i) : carry(found.get(i), union);
                    branches.computeIfAbsent(union.children().get(i), k -> new ArrayList<>()).add(value);
                }
                known = new ReferenceAttribute(
                    union.source(),
                    null,
                    Attribute.rawTemporaryName("promql_label", label, String.valueOf(count())),
                    DataType.KEYWORD
                );
                columns.computeIfAbsent(union, k -> new LinkedHashMap<>()).put(key, known);
                crossed = above;
            }
            aggregate = null;
            origin = union;
            return known;
        }

        /** Records the value of {@code read}: a literal as it is, any other value under a temporary name carried up to the read. */
        private void commit(TimeSeriesMetadataPath read, Expression value) {
            values.put(read, value instanceof Literal ? value : carry(value, read));
        }

        /**
         * {@code value} under a temporary name given right above the node whose output holds what it reads, carried up through
         * the nodes crossed to reach it. A column a union adds has a name of its own already.
         */
        private Attribute carry(Expression value, Node<?> at) {
            if (aggregate != null) {
                List<FieldAttribute> added = keys.computeIfAbsent(aggregate, k -> new ArrayList<>());
                if (added.contains(value) == false) {
                    added.add((FieldAttribute) value);
                }
            }
            Attribute named = origin instanceof UnionAll && value instanceof Attribute attribute
                ? attribute
                : names.computeIfAbsent(origin, k -> new LinkedHashMap<>())
                    .computeIfAbsent(
                        value,
                        v -> new Alias(
                            at.source(),
                            Attribute.rawTemporaryName("promql_label", Expressions.name(v), String.valueOf(count())),
                            v
                        )
                    )
                    .toAttribute();
            for (LogicalPlan node : crossed) {
                if (node != origin) {
                    List<Attribute> added = carried.computeIfAbsent(node, k -> new ArrayList<>());
                    if (added.contains(named) == false) {
                        added.add(named);
                    }
                }
            }
            return named;
        }

        private int count() {
            return names.values().stream().mapToInt(Map::size).sum() + columns.values().stream().mapToInt(Map::size).sum();
        }

        /** Rebuilds {@code node} from the leaves up. */
        private LogicalPlan rewrite(LogicalPlan node) {
            List<LogicalPlan> children = node.children().stream().map(this::rewrite).toList();
            LogicalPlan rewritten = children.equals(node.children()) ? node : node.replaceChildrenSameSize(children);
            Map<String, Attribute> unionColumns = columns.get(node);
            if (unionColumns != null) {
                // each branch gives its values after its own output, under the names of the union's columns, which a union
                // pairs with its branches' columns by
                UnionAll union = (UnionAll) rewritten;
                List<Attribute> added = List.copyOf(unionColumns.values());
                List<LogicalPlan> subPlans = new ArrayList<>();
                for (int i = 0; i < union.children().size(); i++) {
                    LogicalPlan branch = node.children().get(i);
                    List<Alias> given = new ArrayList<>();
                    for (Expression value : branches.get(branch)) {
                        given.add(new Alias(value.source(), added.get(given.size()).name(), value));
                    }
                    List<NamedExpression> output = new ArrayList<>(branch.output());
                    given.forEach(alias -> output.add(alias.toAttribute()));
                    subPlans.add(new Project(branch.source(), new Eval(branch.source(), union.children().get(i), given), output));
                }
                List<Attribute> output = new ArrayList<>(union.output());
                output.addAll(unionColumns.values());
                rewritten = union.replaceSubPlansAndOutput(subPlans, output);
            }
            List<FieldAttribute> grouped = keys.get(node);
            if (grouped != null) {
                TimeSeriesAggregate timeSeriesAggregate = (TimeSeriesAggregate) rewritten;
                List<Expression> groupings = new ArrayList<>(timeSeriesAggregate.groupings());
                List<NamedExpression> aggregates = new ArrayList<>(timeSeriesAggregate.aggregates());
                for (FieldAttribute field : grouped) {
                    if (groupings.contains(field) == false) {
                        groupings.add(field);
                    }
                    if (aggregates.contains(field) == false) {
                        aggregates.add(field);
                    }
                }
                rewritten = timeSeriesAggregate.with(groupings, aggregates);
            }
            List<Attribute> carriedUp = carried.get(node);
            if (carriedUp != null) {
                switch (rewritten) {
                    case Project project -> {
                        List<NamedExpression> projections = new ArrayList<>(project.projections());
                        projections.addAll(carriedUp);
                        rewritten = project.withProjections(projections);
                    }
                    case HashJoin join -> {
                        List<Attribute> added = new ArrayList<>(join.addedFields());
                        added.addAll(carriedUp);
                        rewritten = new HashJoin(
                            join.source(),
                            join.left(),
                            join.right(),
                            join.leftFields(),
                            join.rightFields(),
                            added,
                            join.unique(),
                            join.config().type()
                        );
                    }
                    default -> throw new IllegalStateException("cannot carry a value through [" + rewritten.nodeName() + "]");
                }
            }
            rewritten = rewritten.transformExpressionsOnly(TimeSeriesMetadataPath.class, read -> values.getOrDefault(read, read));
            Map<Expression, Alias> named = names.get(node);
            if (named != null) {
                rewritten = new Eval(node.source(), rewritten, List.copyOf(named.values()));
            }
            return rewritten;
        }

        /** Whether the text of every value of {@code type} has a character, as a write keeps only such text. */
        private static boolean isNeverEmptyText(DataType type) {
            return type.isNumeric()
                || type == DataType.BOOLEAN
                || type == DataType.IP
                || type == DataType.DATETIME
                || type == DataType.DATE_NANOS;
        }

        private static Literal nothing(Expression at) {
            return new Literal(at.source(), null, DataType.KEYWORD);
        }

        /** The label a literal names, null when it isn't a literal. */
        private static String name(Expression label) {
            return label.foldable() && label instanceof Literal literal && literal.value() instanceof BytesRef bytes
                ? bytes.utf8ToString()
                : null;
        }
    }
}
