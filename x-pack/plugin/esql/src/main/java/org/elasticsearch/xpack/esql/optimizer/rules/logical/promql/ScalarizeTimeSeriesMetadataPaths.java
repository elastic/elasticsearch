/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.Build;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The PromQL translation threads the labels of a series through the plan as one value: the {@code _timeseries} label container, of
 * type {@link DataType#TIME_SERIES_METADATA_DATA_TYPE}. A plan does four things with a container, and nothing else:
 * <ul>
 *     <li>{@link TimeSeriesMetadataPath} reads one label by name;</li>
 *     <li>{@link TimeSeriesMetadataFilter} either keeps the labels it names, which reads each of them by name, or leaves the
 *     labels it names out;</li>
 *     <li>{@link TimeSeriesMetadataSet} writes one label;</li>
 *     <li>a grouping, a join key, a top-N key or the output uses the container as a whole.</li>
 * </ul>
 * In this model a container holds every label of its series. What the source loads holds the dimensions only, and reading a
 * label out of it row by row hurts the vectorized operator pipeline. So this rule replaces every read by name with a reference
 * to an attribute loaded next to the container, a compiler optimization called scalar replacement of aggregates (SROA), and loads
 * the container without what nothing needs of it, or not at all. The plan is right only once no read by name is left: a read the
 * rule cannot replace fails the query.
 * <p>
 * The rule knows the container by the calls above alone, not by how the translation uses them. It has two steps
 * ({@link Pushdown}):
 * <ol>
 *     <li>From the root down, changing nothing: for each container, find the labels read by name, and the labels every use of
 *     the container as a whole leaves out.</li>
 *     <li>From the leaves up, each node on its own: a node that defines a container promotes the labels read by name to
 *     attributes next to it, and a node that names a container names the container that is left and its promoted labels.</li>
 * </ol>
 * Where a node before 9.7 takes part ({@link #usesLabelContainer}), a container is the keyword {@code _timeseries} the source
 * loads, and nothing can change it after that. The same two steps run, and a plan that is left with a call changing a container
 * fails.
 */
public final class ScalarizeTimeSeriesMetadataPaths extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return pushDown(plan, context.configuration(), usesLabelContainer(context));
    }

    /**
     * Whether {@code _timeseries} is a typed label container for this query: the feature is on and every participating node reads
     * it. Otherwise what is left of a container stays keyword.
     */
    static boolean usesLabelContainer(AnalyzerContext context) {
        return EsqlCapabilities.Cap.ENABLE_TSMD_DATA_TYPE.isEnabled()
            && DataType.TIME_SERIES_METADATA_DATA_TYPE.supportedVersion()
                .supportedOn(context.minimumVersion(), Build.current().isSnapshot());
    }

    static LogicalPlan pushDown(LogicalPlan plan, Configuration configuration) {
        return pushDown(plan, configuration, true);
    }

    /**
     * {@code plan} with its label containers split into parts; {@code configuration} renders the ones left whole. Without
     * {@code typed} a node before 9.7 takes part: what is left of a container stays the keyword {@code _timeseries} of the source.
     */
    static LogicalPlan pushDown(LogicalPlan plan, Configuration configuration, boolean typed) {
        if (plan.anyMatch(ScalarizeTimeSeriesMetadataPaths::readsSource) == false) {
            return plan;
        }
        return new Pushdown(configuration, typed).apply(plan);
    }

    /** The source's label container, as translation reads it: every label of the series. */
    static boolean isSource(Expression expression) {
        return expression instanceof TimeSeriesWithout without && without.children().isEmpty();
    }

    /** Whether {@code node} is a PromQL time-series aggregate keyed by the source's labels: where every label container starts. */
    private static boolean readsSource(LogicalPlan node) {
        return promqlSource(node) != null
            && ((Aggregate) node).groupings().stream().anyMatch(g -> g instanceof Alias alias && isSource(base(alias.child())));
    }

    /** The input of a PromQL time-series aggregate, where its groupings read the source's labels; null for any other node. */
    private static @Nullable LogicalPlan promqlSource(LogicalPlan node) {
        return node instanceof TimeSeriesAggregate aggregate && aggregate.origin() == TimeSeriesAggregate.Origin.PROMQL_COMMAND
            ? aggregate.child()
            : null;
    }

    /** Whether {@code expression} is a label container. The rule runs before references resolve: an unresolved one has no type. */
    private static boolean isLabelContainer(Expression expression) {
        return expression.resolved() && expression.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE;
    }

    /** The label names a {@code ts_*} call lists after its container. */
    private static List<String> names(List<Expression> arguments) {
        return arguments.stream().map(argument -> BytesRefs.toString(((Literal) argument).value())).toList();
    }

    /** The container a label container expression is built on: what is left under its {@code ts_*} calls. */
    private static Expression base(Expression container) {
        Expression base = container;
        while (base instanceof TimeSeriesMetadataFilter || base instanceof TimeSeriesMetadataSet) {
            base = base.children().getFirst();
        }
        return base;
    }

    /** The join key comparing two label containers, as text. */
    private static final String CONTAINER_KEY = "$prom_rest";

    private static Literal absent(Source source) {
        return new Literal(source, null, DataType.KEYWORD);
    }

    private static boolean isAbsent(Expression expression) {
        return expression instanceof Literal literal && literal.value() == null;
    }

    /** A label value written as empty removes the label, as in Prometheus. */
    private static Expression emptyAsAbsent(Expression value) {
        if (value instanceof Literal literal) {
            return literal.value() == null || BytesRefs.toString(literal.value()).isEmpty() ? absent(value.source()) : value;
        }
        Source at = value.source();
        return new Case(at, new Equals(at, value, Literal.keyword(at, "")), List.of(absent(at), value));
    }

    /** {@code part} as an attribute: a computed one is evaluated by {@code evals}, under a name that shadows no other attribute. */
    private static Attribute attribute(Expression part, String name, List<Alias> evals) {
        if (part instanceof Attribute attribute) {
            return attribute;
        }
        Alias alias = new Alias(part.source(), TemporaryNameGenerator.locallyUniqueTemporaryName(name), part);
        evals.add(alias);
        return alias.toAttribute();
    }

    private static <T extends NamedExpression> void addIfAbsent(List<? super T> list, T expression) {
        for (Object present : list) {
            if (present instanceof NamedExpression named && named.id().equals(expression.id())) {
                return;
            }
        }
        list.add(expression);
    }

    /** {@code child} with {@code evals} evaluated above it. */
    private static LogicalPlan evaluate(LogicalPlan child, List<Alias> evals) {
        return evals.isEmpty() ? child : new Eval(child.source(), child, evals);
    }

    /**
     * {@code child} with {@code labels} packed into one key, which {@code keys} gets; a label of several values keys as a whole.
     * The key stays in the output of the node it keys, so it has a name of its own: a node above may pack a key too.
     */
    private static LogicalPlan pack(LogicalPlan child, List<Attribute> labels, List<? super Attribute> keys) {
        if (labels.isEmpty()) {
            return child;
        }
        String name = TemporaryNameGenerator.locallyUniqueTemporaryName(PackDims.PACKED_FIELD_NAME);
        Attribute packed = new ReferenceAttribute(child.source(), null, name, DataType.KEYWORD);
        keys.add(packed);
        return new PackDims(child.source(), child, labels, packed);
    }

    /**
     * One run of the rule over one plan. All state is kept by the id of a label container attribute of the plan: step 1 fills what
     * the plan asks of each container, step 2 what each container is in the rewritten plan.
     */
    static final class Pushdown {
        private final Configuration configuration;
        /** Whether every node reads the typed label container; otherwise a container stays the keyword the source loads. */
        private final boolean typed;

        /** Step 1: the labels read by name off each container. */
        private final Map<NameId, Set<String>> need = new HashMap<>();
        /** Step 1: the labels every use of each container as a whole leaves out. No entry: nothing uses it as a whole. */
        private final Map<NameId, Set<String>> tolerated = new HashMap<>();
        /** The expression each alias of the plan names. */
        private final Map<NameId, Expression> definitions = new HashMap<>();
        /** The branch attributes each union output takes its value from. */
        private final Map<NameId, List<Attribute>> unions = new HashMap<>();

        /** Step 2: each container itself, as an expression over the input of the node at hand. No entry: nothing is left of it. */
        private final Map<NameId, Expression> timeseries = new HashMap<>();
        /** Step 2: the labels each container is without: left out of it, written over, or never in it. */
        private final Map<NameId, Set<String>> without = new HashMap<>();
        /**
         * Step 2: the labels promoted out of each container, each held by an expression of its own. A promoted label the container
         * is not {@link #without} is in both, alike. A label neither promoted nor in the container is absent.
         */
        private final Map<NameId, SortedMap<String, Expression>> promoted = new HashMap<>();
        /** The input of the time-series aggregate that loads each of these container attributes. */
        private final Map<NameId, LogicalPlan> sources = new HashMap<>();
        /** The input of the time-series aggregate whose groupings are being rewritten: where the source's labels are read. */
        private LogicalPlan source;

        Pushdown(Configuration configuration, boolean typed) {
            this.configuration = configuration;
            this.typed = typed;
        }

        LogicalPlan apply(LogicalPlan plan) {
            demand(plan);
            LogicalPlan rewritten = rewrite(plan);
            if (typed == false) {
                // A keyword container cannot change after the source loads it: no call that changes one may be left.
                rewritten.forEachDown(node -> node.forEachExpression(Expression.class, e -> {
                    if (e instanceof TimeSeriesMetadataFilter || e instanceof TimeSeriesMetadataSet) {
                        throw new VerificationException(
                            List.of(
                                Failure.fail(e, "leaving labels out of _timeseries requires a typed label container [{}]", e.sourceText())
                            )
                        );
                    }
                }));
            }
            return rewritten;
        }

        // -- step 1: what the plan asks of its containers. From the root down, so every use is seen before the definition --

        Set<String> need(NameId id) {
            return need.getOrDefault(id, Set.of());
        }

        @Nullable
        Set<String> tolerated(NameId id) {
            return tolerated.get(id);
        }

        void demand(LogicalPlan plan) {
            plan.forEachDown(node -> {
                for (Expression expression : node.expressions()) {
                    if (expression instanceof Alias alias) {
                        definitions.put(alias.id(), alias.child());
                    }
                }
                if (node instanceof UnionAll union) {
                    for (int i = 0; i < union.output().size(); i++) {
                        var branches = new ArrayList<Attribute>();
                        for (LogicalPlan child : union.children()) {
                            branches.add(child.output().get(i));
                        }
                        unions.put(union.output().get(i).id(), branches);
                    }
                }
            });
            plan.forEachDown(node -> {
                if (node instanceof UnionAll union) {
                    for (int i = 0; i < union.output().size(); i++) {
                        Attribute out = union.output().get(i);
                        if (isLabelContainer(out) == false) {
                            continue;
                        }
                        // every branch returns the labels any branch promotes
                        Set<String> labels = need.computeIfAbsent(out.id(), k -> new TreeSet<>());
                        labels.addAll(written(out));
                        for (LogicalPlan child : union.children()) {
                            Attribute branch = child.output().get(i);
                            if (isLabelContainer(branch)) {
                                List.copyOf(labels).forEach(label -> read(branch, label));
                                use(branch, tolerated.get(out.id()));
                            }
                        }
                    }
                    return;
                }
                if (node instanceof HashJoin join) {
                    for (int i = 0; i < join.leftFields().size(); i++) {
                        Attribute left = join.leftFields().get(i);
                        Attribute right = join.rightFields().get(i);
                        if (isLabelContainer(left) && isLabelContainer(right)) {
                            // both sides compare by name the labels either promotes
                            var labels = new TreeSet<>(written(left));
                            labels.addAll(written(right));
                            for (String label : labels) {
                                read(left, label);
                                read(right, label);
                            }
                        }
                        // a key is compared: nothing of it may be missing
                        use(left, Set.of());
                        use(right, Set.of());
                    }
                    return;
                }
                List<Expression> keys = switch (node) {
                    case Aggregate aggregate -> aggregate.groupings();
                    case TopNBy topNBy -> topNBy.groupings();
                    default -> List.of();
                };
                for (Expression key : keys) {
                    if (key instanceof Attribute) {
                        use(key, Set.of());
                    }
                }
                List<? extends Expression> expressions = node.expressions();
                // from the last one: a field may read a container an earlier field of the same node defines
                for (int i = expressions.size() - 1; i >= 0; i--) {
                    Expression expression = expressions.get(i);
                    if (expression instanceof Attribute) {
                        // the attribute passes through
                        continue;
                    }
                    if (expression instanceof Alias alias && isLabelContainer(alias)) {
                        boolean key = node instanceof Aggregate && keys.contains(alias);
                        Set<String> out = tolerated.get(alias.id());
                        if (out == null && key) {
                            // a key is compared: every label it holds is read, whatever reads the key above
                            out = Set.of();
                        }
                        // a label read off the alias is read off its definition
                        for (String label : List.copyOf(need(alias.id()))) {
                            read(alias.child(), label);
                        }
                        if (key) {
                            // a label every use of the key leaves out keys on its own, so the groups stay the same
                            for (String label : out) {
                                read(alias.child(), label);
                            }
                        }
                        use(alias.child(), out);
                    } else {
                        scan(expression);
                    }
                }
            });
        }

        /** The uses {@code expression} makes of containers: a label read by name, or the container as a whole. */
        private void scan(Expression expression) {
            if (expression instanceof TimeSeriesMetadataPath path) {
                read(path.children().getFirst(), names(path.children().subList(1, 2)).getFirst());
                return;
            }
            if (base(expression) instanceof Attribute attribute && isLabelContainer(attribute)) {
                use(expression, Set.of());
                return;
            }
            expression.children().forEach(this::scan);
        }

        /** {@code label} is read by name off {@code container}: the container attribute under its {@code ts_*} calls returns it. */
        private void read(Expression container, String label) {
            switch (container) {
                case TimeSeriesMetadataFilter filter when filter.exclude() -> {
                    if (names(filter.labels()).contains(label) == false) {
                        read(filter.children().getFirst(), label);
                    }
                }
                // TS_KEEP reads every label it names
                case TimeSeriesMetadataFilter filter -> names(filter.labels()).forEach(kept -> read(filter.children().getFirst(), kept));
                case TimeSeriesMetadataSet set -> {
                    scan(set.value());
                    if (names(List.of(set.label())).getFirst().equals(label) == false) {
                        read(set.children().getFirst(), label);
                    }
                }
                case Attribute attribute when isLabelContainer(attribute) -> need.computeIfAbsent(attribute.id(), k -> new TreeSet<>())
                    .add(label);
                // anything else uses as a whole what it reads
                default -> container.children().forEach(this::scan);
            }
        }

        /** {@code container} is used as a whole, by a use that leaves {@code out} out of it; null when nothing uses the result. */
        private void use(Expression container, @Nullable Set<String> out) {
            switch (container) {
                case Attribute attribute when isLabelContainer(attribute) -> {
                    if (out != null) {
                        Set<String> known = tolerated.get(attribute.id());
                        if (known == null) {
                            tolerated.put(attribute.id(), new TreeSet<>(out));
                        } else {
                            known.retainAll(out);
                        }
                    }
                }
                case TimeSeriesMetadataFilter filter when filter.exclude() -> {
                    Set<String> more = out == null ? null : new TreeSet<>(out);
                    if (more != null) {
                        more.addAll(names(filter.labels()));
                    }
                    use(filter.children().getFirst(), more);
                }
                // TS_KEEP reads the labels it names, by name: nothing else of its container is used
                case TimeSeriesMetadataFilter filter -> names(filter.labels()).forEach(kept -> read(filter.children().getFirst(), kept));
                case TimeSeriesMetadataSet set -> {
                    Set<String> more = out == null ? null : new TreeSet<>(out);
                    if (more != null) {
                        more.addAll(names(List.of(set.label())));
                    }
                    use(set.children().getFirst(), more);
                    scan(set.value());
                }
                // anything else uses as a whole what it reads
                default -> container.children().forEach(this::scan);
            }
        }

        /**
         * The labels {@code container} promotes whatever is read off it: the ones a TS_KEEP keeps or a TS_SET writes on the way from
         * the node that loads it. Two containers compared as a whole compare these by name.
         */
        private Set<String> written(Expression container) {
            var written = new TreeSet<String>();
            switch (container) {
                case TimeSeriesMetadataFilter filter when filter.exclude() -> {
                    written.addAll(written(filter.children().getFirst()));
                    written.removeAll(names(filter.labels()));
                }
                case TimeSeriesMetadataFilter filter -> written.addAll(names(filter.labels()));
                case TimeSeriesMetadataSet set -> {
                    written.addAll(written(set.children().getFirst()));
                    written.addAll(names(List.of(set.label())));
                }
                case Attribute attribute -> {
                    Expression definition = definitions.get(attribute.id());
                    if (definition != null) {
                        written.addAll(written(definition));
                    }
                    for (Attribute branch : unions.getOrDefault(attribute.id(), List.of())) {
                        // the union output has the id of its first branch
                        if (branch.id().equals(attribute.id()) == false) {
                            written.addAll(written(branch));
                        }
                    }
                }
                default -> {
                }
            }
            return written;
        }

        // -- step 2: the rewrite. From the leaves up, each node on its own --

        /**
         * Step 2. The children first, then the node: a node that defines a container records what it is ({@link #define}), and a
         * node that names a container names the container that is left and its promoted labels instead.
         */
        private LogicalPlan rewrite(LogicalPlan node) {
            List<LogicalPlan> children = node.children().stream().map(this::rewrite).toList();
            // the union reads the outputs of its branches as they were, by position
            LogicalPlan rewritten = node instanceof UnionAll || children.equals(node.children())
                ? node
                : node.replaceChildrenSameSize(children);
            switch (rewritten) {
                // The union with each label container position replaced by its parts. Every branch returns the same parts: its container,
                // if any branch has one, and each label step 1 asked every branch for.
                case UnionAll union -> {
                    var evals = new ArrayList<List<Alias>>();
                    var projections = new ArrayList<List<NamedExpression>>();
                    for (int j = 0; j < children.size(); j++) {
                        evals.add(new ArrayList<>());
                        projections.add(new ArrayList<>());
                    }
                    var output = new ArrayList<Attribute>();
                    // The union output of each list of branch attributes: a page holds each attribute once, so a branch projecting the
                    // same attribute twice would leave the union an output short.
                    var outputs = new HashMap<List<NameId>, Attribute>();
                    // the parts of each container position, as union outputs: recorded once every position has read its branches
                    var containers = new HashMap<NameId, Attribute>();
                    var gone = new HashMap<NameId, Set<String>>();
                    var labels = new HashMap<NameId, SortedMap<String, Expression>>();
                    for (int i = 0; i < union.output().size(); i++) {
                        Attribute out = union.output().get(i);
                        if (isContainer(out) == false) {
                            output.add(out);
                            for (int j = 0; j < children.size(); j++) {
                                projections.get(j).add(union.children().get(j).output().get(i));
                            }
                            continue;
                        }
                        Source at = out.source();
                        var branches = new ArrayList<NameId>();
                        for (int j = 0; j < children.size(); j++) {
                            NameId branch = union.children().get(j).output().get(i).id();
                            if (promoted.containsKey(branch)) {
                                materialize(branch, evals.get(j));
                            }
                            branches.add(branch);
                        }
                        // what every branch returns for this position: the name of each part, its type, and the part of each branch
                        var names = new ArrayList<String>();
                        var types = new ArrayList<DataType>();
                        var parts = new ArrayList<List<Expression>>();
                        // The union's container is without a label only where every branch's is. A branch without a container
                        // returns one holding its promoted labels, as the other branches' containers hold theirs.
                        var outGone = new TreeSet<String>();
                        boolean container = false;
                        for (NameId branch : branches) {
                            if (timeseries.containsKey(branch)) {
                                if (container) {
                                    outGone.retainAll(without.get(branch));
                                } else {
                                    outGone.addAll(without.get(branch));
                                }
                                container = true;
                            }
                        }
                        for (NameId branch : branches) {
                            if (timeseries.containsKey(branch) == false) {
                                outGone.removeAll(promoted.getOrDefault(branch, new TreeMap<>()).keySet());
                            }
                        }
                        if (container) {
                            var branchParts = new ArrayList<Expression>();
                            for (NameId branch : branches) {
                                branchParts.add(timeseries.containsKey(branch) ? timeseries.get(branch) : whole(branch, at));
                            }
                            // Union outputs are named apart: the container's own name for the container, prefixed for its labels.
                            names.add(out.name());
                            types.add(typed ? DataType.TIME_SERIES_METADATA_DATA_TYPE : DataType.KEYWORD);
                            parts.add(branchParts);
                        }
                        var outLabels = new ArrayList<String>();
                        for (String name : need(out.id())) {
                            var branchParts = new ArrayList<Expression>();
                            for (NameId branch : branches) {
                                branchParts.add(
                                    label(
                                        timeseries.get(branch),
                                        without.getOrDefault(branch, Set.of()),
                                        promoted.getOrDefault(branch, new TreeMap<>()),
                                        name,
                                        at
                                    )
                                );
                            }
                            if (branchParts.stream().allMatch(ScalarizeTimeSeriesMetadataPaths::isAbsent)) {
                                // no branch holds the label: the container is without it
                                outGone.add(name);
                            } else {
                                outLabels.add(name);
                                names.add(out.name() + "." + name);
                                types.add(DataType.KEYWORD);
                                parts.add(branchParts);
                            }
                        }
                        SortedMap<String, Expression> outPromoted = new TreeMap<>();
                        for (int p = 0; p < parts.size(); p++) {
                            String name = names.get(p);
                            var attributes = new ArrayList<Attribute>();
                            for (int j = 0; j < children.size(); j++) {
                                attributes.add(attribute(parts.get(p).get(j), name, evals.get(j)));
                            }
                            List<NameId> key = attributes.stream().map(Attribute::id).toList();
                            Attribute part = outputs.get(key);
                            if (part == null) {
                                part = new ReferenceAttribute(
                                    parts.get(p).getFirst().source(),
                                    null,
                                    name,
                                    types.get(p),
                                    Nullability.TRUE,
                                    null,
                                    false
                                );
                                output.add(part);
                                outputs.put(key, part);
                                for (int j = 0; j < children.size(); j++) {
                                    Attribute attribute = attributes.get(j);
                                    projections.get(j)
                                        .add(attribute.name().equals(name) ? attribute : new Alias(attribute.source(), name, attribute));
                                }
                            }
                            if (container && p == 0) {
                                containers.put(out.id(), part);
                            } else {
                                outPromoted.put(outLabels.get(container ? p - 1 : p), part);
                            }
                        }
                        gone.put(out.id(), outGone);
                        labels.put(out.id(), outPromoted);
                    }
                    var branches = new ArrayList<LogicalPlan>();
                    for (int j = 0; j < children.size(); j++) {
                        LogicalPlan child = evaluate(children.get(j), evals.get(j));
                        branches.add(new Project(child.source(), child, projections.get(j)));
                    }
                    UnionAll unionAll = new UnionAll(union.source(), branches, output);
                    // The parts leave the union under ids of their own: the analyzer pushes a conversion of a union output, such as the
                    // rendering of the label container, into the branches, and would leave its attribute in the command's output.
                    var renamed = new HashMap<NameId, Attribute>();
                    var leaving = new ArrayList<NamedExpression>();
                    for (Attribute attribute : output) {
                        if (outputs.containsValue(attribute)) {
                            Alias alias = new Alias(attribute.source(), attribute.name(), attribute);
                            renamed.put(attribute.id(), alias.toAttribute());
                            leaving.add(alias);
                        } else {
                            leaving.add(attribute);
                        }
                    }
                    labels.forEach((id, outPromoted) -> {
                        if (containers.containsKey(id)) {
                            timeseries.put(id, renamed.get(containers.get(id).id()));
                        } else {
                            timeseries.remove(id);
                        }
                        outPromoted.replaceAll((name, label) -> renamed.get(((Attribute) label).id()));
                        without.put(id, gone.get(id));
                        promoted.put(id, outPromoted);
                    });
                    return new Project(union.source(), unionAll, leaving);
                }
                // The aggregate keyed by the parts of its label containers: each container, and its promoted labels. A container leaves
                // out the labels every use above leaves out, which become keys of their own so the groups stay the same. The time-series
                // aggregate keys by the labels as they are and its translation packs them; any other aggregate packs them itself: a label
                // of several values keys as a whole.
                case Aggregate aggregate -> {
                    boolean timeSeries = aggregate instanceof TimeSeriesAggregate;
                    var evals = new ArrayList<Alias>();
                    var groupings = new ArrayList<Expression>();
                    var keys = new ArrayList<NameId>();
                    var packedLabels = new ArrayList<Attribute>();
                    source = promqlSource(aggregate);
                    for (Expression grouping : aggregate.groupings()) {
                        if ((grouping instanceof NamedExpression named && isContainer(named)) == false) {
                            groupings.add(grouping);
                            continue;
                        }
                        NamedExpression key = (NamedExpression) grouping;
                        NameId id = key.id();
                        Expression definition = key instanceof Alias alias ? alias.child() : key;
                        define(id, definition);
                        SortedMap<String, Expression> labels = promoted.get(id);
                        Set<String> out = tolerated.get(id);
                        // The aggregate returns what it groups by: a container stays a key of it, whether or not anything above uses
                        // it. A label every use above leaves out keys on its own, and the container is without it: the key still
                        // tells apart what the container told apart. Of the source's labels only a dimension is in the container.
                        Expression whole = timeseries.get(id);
                        if (whole != null && out != null) {
                            var split = new TreeSet<String>();
                            for (String name : out) {
                                if (without.get(id).contains(name) || isSource(whole) && PromqlLabels.getLabel(source, name) == null) {
                                    continue;
                                }
                                Expression value = labels.get(name);
                                if (value == null && isSource(whole)) {
                                    value = PromqlLabels.getField(source, name);
                                }
                                if (value != null && isAbsent(value) == false) {
                                    labels.put(name, value);
                                    split.add(name);
                                }
                            }
                            if (split.isEmpty() == false) {
                                timeseries.put(id, unset(whole, without.get(id), split, Source.EMPTY));
                                without.get(id).addAll(split);
                            }
                        }
                        dropCopies(id);
                        if (timeSeries) {
                            // the time-series aggregate keys by an expression as it is
                            var aliases = new ArrayList<NamedExpression>();
                            Expression container = container(id);
                            if (container != null) {
                                NamedExpression loaded = container instanceof Attribute attribute
                                    ? attribute
                                    : new Alias(container.source(), MetadataAttribute.TIMESERIES, container);
                                addIfAbsent(aliases, loaded);
                                if (isSource(timeseries.get(id))) {
                                    sources.put(loaded.id(), source);
                                }
                                timeseries.put(id, loaded.toAttribute());
                            }
                            for (var label : labels.entrySet()) {
                                Expression part = label.getValue();
                                NamedExpression keyed = null;
                                if (part instanceof Attribute attribute && attribute instanceof FieldAttribute == false) {
                                    keyed = attribute;
                                } else {
                                    // A source field keyed on gets an id of its own: above the aggregate it is a reference, not the
                                    // field. A grouping that keys by it already is that key.
                                    for (Expression existing : aggregate.groupings()) {
                                        if (keyed == null && existing instanceof Alias alias && alias.child().semanticEquals(part)) {
                                            keyed = alias;
                                        }
                                    }
                                    if (keyed == null) {
                                        keyed = new Alias(part.source(), label.getKey(), part);
                                    }
                                }
                                addIfAbsent(aliases, keyed);
                                label.setValue(keyed.toAttribute());
                            }
                            groupings.addAll(aliases);
                        } else {
                            materialize(id, evals);
                            if (timeseries.containsKey(id)) {
                                groupings.add(timeseries.get(id));
                            }
                            labels.values().forEach(label -> addIfAbsent(packedLabels, (Attribute) label));
                        }
                        keys.add(id);
                    }
                    source = null;
                    if (keys.isEmpty()) {
                        return aggregate.transformExpressionsOnly(Expression.class, this::scalar);
                    }
                    LogicalPlan child = evaluate(aggregate.child(), evals);
                    // the aggregates, with each container key replaced by its parts; the packed labels leave with the packed key
                    var aggregates = new ArrayList<NamedExpression>();
                    for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                        NameId id = aggregateExpression.id();
                        if (keys.contains(id) == false) {
                            if (packedLabels.stream().noneMatch(label -> label.id().equals(id))) {
                                addIfAbsent(aggregates, (NamedExpression) scalar(aggregateExpression));
                            }
                            continue;
                        }
                        if (timeseries.containsKey(id)) {
                            addIfAbsent(aggregates, (Attribute) timeseries.get(id));
                        }
                        if (packedLabels.isEmpty()) {
                            promoted.get(id).values().forEach(label -> addIfAbsent(aggregates, (Attribute) label));
                        }
                    }
                    if (packedLabels.isEmpty()) {
                        var distinct = new ArrayList<Expression>();
                        for (Expression grouping : groupings) {
                            if (grouping instanceof NamedExpression named
                                && distinct.stream().anyMatch(g -> g instanceof NamedExpression n && n.id().equals(named.id()))) {
                                continue;
                            }
                            distinct.add(grouping);
                        }
                        return aggregate.with(child, distinct, aggregates);
                    }
                    Source at = aggregate.source();
                    PackDims pack = new PackDims(at, child, packedLabels, PackDims.newPackedAttribute(at));
                    Alias packed = PackDims.newPackedGrouping(at, pack.packed());
                    groupings.add(packed);
                    aggregates.add(packed.toAttribute());
                    Aggregate packedAggregate = aggregate.with(pack, groupings, aggregates);
                    // an unpacked label keeps its id: what reads it above the aggregate reads the unpacked attribute
                    var unpacked = new LinkedHashMap<NameId, Attribute>();
                    for (Attribute label : packedLabels) {
                        unpacked.put(
                            label.id(),
                            new ReferenceAttribute(
                                label.source(),
                                null,
                                label.name(),
                                label.dataType().noText(),
                                Nullability.TRUE,
                                label.id(),
                                false
                            )
                        );
                    }
                    var output = new ArrayList<NamedExpression>();
                    for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                        NameId id = aggregateExpression.id();
                        if (keys.contains(id) == false) {
                            Attribute label = unpacked.get(id);
                            addIfAbsent(output, label != null ? label : aggregateExpression.toAttribute());
                            continue;
                        }
                        if (timeseries.containsKey(id)) {
                            addIfAbsent(output, (Attribute) timeseries.get(id));
                        }
                        promoted.get(id).replaceAll((name, label) -> unpacked.get(((Attribute) label).id()));
                        promoted.get(id).values().forEach(label -> addIfAbsent(output, (Attribute) label));
                    }
                    return new Project(
                        at,
                        new UnpackDims(at, packedAggregate, packed.toAttribute(), List.copyOf(unpacked.values())),
                        output
                    );
                }
                // A label container is not evaluated: what names it above names its parts. A label read by name is evaluated once, here,
                // where its container is defined.
                case Eval eval -> {
                    var fields = new ArrayList<Alias>();
                    for (Alias field : eval.fields()) {
                        if (isContainer(field)) {
                            define(field.id(), field.child());
                            promoted.get(field.id())
                                .replaceAll((name, value) -> need(field.id()).contains(name) ? attribute(value, name, fields) : value);
                        } else {
                            fields.add((Alias) scalar(field));
                        }
                    }
                    if (fields.equals(eval.fields())) {
                        return eval;
                    }
                    return fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
                }
                // A projection keeps the parts of the containers it projects.
                case Project project -> {
                    var evals = new ArrayList<Alias>();
                    var projections = new ArrayList<NamedExpression>();
                    for (NamedExpression projection : project.projections()) {
                        if (isContainer(projection) == false) {
                            addIfAbsent(projections, (NamedExpression) scalar(projection));
                            continue;
                        }
                        NameId id = projection.id();
                        if (projection instanceof Alias alias) {
                            define(id, alias.child());
                        } else {
                            record(projection.toAttribute());
                        }
                        dropCopies(id);
                        materialize(id, evals);
                        if (timeseries.containsKey(id)) {
                            addIfAbsent(projections, (Attribute) timeseries.get(id));
                        }
                        promoted.get(id).replaceAll((name, label) -> {
                            Attribute attribute = (Attribute) label;
                            // The analyzer resolves a reference above by name: a label must not share the name of another projection.
                            boolean shadows = project.projections()
                                .stream()
                                .anyMatch(p -> p.name().equals(attribute.name()) && p.id().equals(attribute.id()) == false)
                                || projections.stream()
                                    .anyMatch(p -> p.name().equals(attribute.name()) && p.id().equals(attribute.id()) == false);
                            NamedExpression projected = shadows
                                ? new Alias(attribute.source(), TemporaryNameGenerator.locallyUniqueTemporaryName(name), attribute)
                                : attribute;
                            addIfAbsent(projections, projected);
                            return projected.toAttribute();
                        });
                    }
                    LogicalPlan child = evaluate(project.child(), evals);
                    return new Project(project.source(), child, projections);
                }
                // The join on the parts of its label container keys: the containers as text, the promoted labels packed.
                case HashJoin join -> {
                    var leftEvals = new ArrayList<Alias>();
                    var rightEvals = new ArrayList<Alias>();
                    var leftLabels = new ArrayList<Attribute>();
                    var rightLabels = new ArrayList<Attribute>();
                    var leftFields = new ArrayList<Attribute>();
                    var rightFields = new ArrayList<Attribute>();
                    for (int i = 0; i < join.leftFields().size(); i++) {
                        Attribute left = join.leftFields().get(i);
                        Attribute right = join.rightFields().get(i);
                        if ((isContainer(left) && isContainer(right)) == false) {
                            leftFields.add(left);
                            rightFields.add(right);
                            continue;
                        }
                        NameId l = record(left);
                        NameId r = record(right);
                        // both sides compare by name the labels either holds outside its container
                        var names = new TreeSet<String>();
                        promoted.get(l).keySet().stream().filter(name -> inContainer(l, name) == false).forEach(names::add);
                        promoted.get(r).keySet().stream().filter(name -> inContainer(r, name) == false).forEach(names::add);
                        for (String name : names) {
                            Expression leftLabel = label(timeseries.get(l), without.get(l), promoted.get(l), name, left.source());
                            Expression rightLabel = label(timeseries.get(r), without.get(r), promoted.get(r), name, right.source());
                            leftLabels.add(attribute(leftLabel, name, leftEvals));
                            rightLabels.add(attribute(rightLabel, name, rightEvals));
                        }
                        if (timeseries.containsKey(l) || timeseries.containsKey(r)) {
                            // A label one side holds in its container too and the other outside it compares packed only: that side's
                            // container leaves it out.
                            var leftOut = new TreeSet<String>();
                            var rightOut = new TreeSet<String>();
                            for (String name : typed ? names : Set.<String>of()) {
                                if (inContainer(l, name) && inContainer(r, name) == false) {
                                    leftOut.add(name);
                                } else if (inContainer(r, name) && inContainer(l, name) == false) {
                                    rightOut.add(name);
                                }
                            }
                            leftFields.add(attribute(containerKey(l, leftOut, left.source()), CONTAINER_KEY, leftEvals));
                            rightFields.add(attribute(containerKey(r, rightOut, right.source()), CONTAINER_KEY, rightEvals));
                        }
                    }
                    LogicalPlan leftChild = evaluate(join.left(), leftEvals);
                    LogicalPlan rightChild = evaluate(join.right(), rightEvals);
                    rightChild = pack(rightChild, rightLabels, rightFields);
                    // The join merges its output by name: the right side keeps its keys and what it adds, a container's parts under the
                    // container's name, so they cannot shadow the left side's labels.
                    var projections = new ArrayList<NamedExpression>(rightFields);
                    var added = new ArrayList<Attribute>();
                    for (Attribute attribute : join.addedFields()) {
                        if (isContainer(attribute) == false) {
                            projections.add(attribute);
                            added.add(attribute);
                            continue;
                        }
                        NameId id = record(attribute);
                        dropCopies(id);
                        Expression container = container(id);
                        if (container != null) {
                            Alias alias = new Alias(container.source(), attribute.name(), container);
                            projections.add(alias);
                            added.add(alias.toAttribute());
                            timeseries.put(id, alias.toAttribute());
                        }
                        promoted.get(id).replaceAll((name, label) -> {
                            Alias alias = new Alias(label.source(), attribute.name() + "." + name, label);
                            projections.add(alias);
                            added.add(alias.toAttribute());
                            return alias.toAttribute();
                        });
                    }
                    return new HashJoin(
                        join.source(),
                        pack(leftChild, leftLabels, leftFields),
                        new Project(rightChild.source(), rightChild, projections),
                        leftFields,
                        rightFields,
                        added,
                        join.unique(),
                        join.config().type()
                    );
                }
                case TopNBy topNBy -> {
                    var evals = new ArrayList<Alias>();
                    var labels = new ArrayList<Attribute>();
                    var groupings = new ArrayList<Expression>();
                    for (Expression grouping : topNBy.groupings()) {
                        if ((grouping instanceof Attribute attribute && isContainer(attribute)) == false) {
                            groupings.add(scalar(grouping));
                            continue;
                        }
                        NameId id = record(grouping);
                        materialize(id, evals);
                        if (timeseries.containsKey(id)) {
                            groupings.add(timeseries.get(id));
                        }
                        // a label the container holds too is compared with it
                        promoted.get(id).forEach((name, label) -> {
                            if (inContainer(id, name) == false) {
                                addIfAbsent(labels, (Attribute) label);
                            }
                        });
                    }
                    LogicalPlan child = evaluate(topNBy.child(), evals);
                    return new TopNBy(topNBy.source(), pack(child, labels, groupings), topNBy.order(), topNBy.limitPerGroup(), groupings);
                }
                default -> {
                    return rewritten.transformExpressionsOnlyUp(Expression.class, this::scalar);
                }
            }
        }

        private boolean isContainer(NamedExpression named) {
            return promoted.containsKey(named.id()) || isLabelContainer(named);
        }

        /** Whether {@code expression} reads a label container of the plan, or the source's labels within its aggregate. */
        private boolean reads(Expression expression) {
            return expression.anyMatch(e -> e instanceof Attribute attribute && isContainer(attribute))
                || (source != null && expression.anyMatch(ScalarizeTimeSeriesMetadataPaths::isSource));
        }

        /**
         * Records under {@code id} what the label container expression {@code definition} is: {@link #timeseries},
         * {@link #without} and {@link #promoted}. Starts from the container under the {@code ts_*} calls of {@code definition}
         * and applies the calls from that one up. Then promotes each label step 1 found read by name off {@code id}.
         */
        private void define(NameId id, Expression definition) {
            var calls = new ArrayDeque<Expression>();
            Expression under = definition;
            while (under instanceof TimeSeriesMetadataFilter || under instanceof TimeSeriesMetadataSet) {
                calls.push(under);
                under = under.children().getFirst();
            }
            Expression container;
            var gone = new TreeSet<String>();
            SortedMap<String, Expression> labels = new TreeMap<>();
            if (under instanceof Attribute attribute && promoted.containsKey(attribute.id())) {
                container = timeseries.get(attribute.id());
                gone.addAll(without.get(attribute.id()));
                labels.putAll(promoted.get(attribute.id()));
            } else if (under instanceof Attribute attribute && isLabelContainer(attribute)) {
                // a container no node below defines: it is whole
                container = attribute;
            } else if (isSource(under)) {
                if (source == null) {
                    throw new IllegalStateException("the source's label container is read outside of its aggregate");
                }
                // every label of the series; what it is without is left out where the aggregate loads it
                container = under;
            } else if (under instanceof Literal) {
                // the empty container
                container = null;
            } else {
                throw new IllegalStateException("unexpected label container [" + definition + "]");
            }
            for (Expression call : calls) {
                switch (call) {
                    // TS_UNSET: neither the container nor a promoted label holds the names any more
                    case TimeSeriesMetadataFilter filter when filter.exclude() -> {
                        List<String> names = names(filter.labels());
                        container = unset(container, gone, names, call.source());
                        gone.addAll(names);
                        labels.keySet().removeAll(names);
                    }
                    // TS_KEEP: the read of each name. Nothing is left of the container
                    case TimeSeriesMetadataFilter filter -> {
                        SortedMap<String, Expression> kept = new TreeMap<>();
                        for (String name : names(filter.labels())) {
                            Expression value = label(container, gone, labels, name, call.source());
                            if (isAbsent(value) == false) {
                                kept.put(name, value);
                            }
                        }
                        container = null;
                        gone.clear();
                        labels = kept;
                    }
                    // TS_SET: the value is promoted, and the container is without the label. Writing null removes the label
                    case TimeSeriesMetadataSet set -> {
                        String name = names(List.of(set.label())).getFirst();
                        Expression value = emptyAsAbsent(scalar(set.value()));
                        container = unset(container, gone, List.of(name), call.source());
                        gone.add(name);
                        labels.remove(name);
                        if (isAbsent(value) == false) {
                            labels.put(name, value);
                        }
                    }
                    default -> throw new IllegalStateException("unexpected label container call [" + call + "]");
                }
            }
            for (String name : need(id)) {
                if (labels.containsKey(name) == false) {
                    Expression value = label(container, gone, labels, name, Source.EMPTY);
                    if (isAbsent(value) == false) {
                        labels.put(name, value);
                    } else if (container != null) {
                        // the source has no field for it: it is in no container
                        gone.add(name);
                    }
                }
            }
            if (container == null) {
                timeseries.remove(id);
            } else {
                timeseries.put(id, container);
            }
            without.put(id, gone);
            promoted.put(id, labels);
        }

        /**
         * The label {@code name} of a container: the expression it is promoted to, null when the container cannot hold it, or the
         * source's field where the aggregate at hand loads the container. A label is never read off a container: step 1 has every
         * label read by name promoted by the node that defines the container, so one that is not there cannot be read.
         */
        private Expression label(@Nullable Expression container, Set<String> gone, Map<String, Expression> labels, String name, Source at) {
            Expression value = labels.get(name);
            if (value != null) {
                return value;
            }
            if (container == null || gone.contains(name)) {
                return absent(at);
            }
            if (isSource(container)) {
                Attribute field = PromqlLabels.getField(source, name);
                return field == null ? absent(at) : field;
            }
            throw new IllegalStateException("label [" + name + "] is read off [" + container + "], which does not return it");
        }

        /**
         * {@code container}, already without {@code gone}, without {@code names} as well. The source's labels leave them out where
         * the aggregate loads them. A container a time-series aggregate loads holds dimensions only, so it is without any other
         * name already; where it is keyword it cannot change, and a use that leaves out more reads it as it is, as the keyword
         * translation did.
         */
        private @Nullable Expression unset(@Nullable Expression container, Set<String> gone, Collection<String> names, Source at) {
            if (container == null || isSource(container) || gone.containsAll(names)) {
                return container;
            }
            var left = new LinkedHashSet<>(names);
            left.removeAll(gone);
            LogicalPlan series = base(container) instanceof Attribute loaded ? sources.get(loaded.id()) : null;
            if (series != null) {
                left.removeIf(name -> PromqlLabels.getLabel(series, name) == null);
                if (left.isEmpty() || typed == false) {
                    return container;
                }
            }
            return new TimeSeriesMetadataFilter(
                at,
                container,
                left.stream().<Expression>map(name -> Literal.keyword(at, name)).toList(),
                true
            );
        }

        /** The id {@code container} is recorded under: its own for an attribute, a new one for any other container expression. */
        private NameId record(Expression container) {
            if (container instanceof Attribute attribute) {
                if (promoted.containsKey(attribute.id()) == false) {
                    define(attribute.id(), attribute);
                }
                return attribute.id();
            }
            NameId id = new NameId();
            define(id, container);
            return id;
        }

        /** The container {@code id} as an expression, null when nothing is left of it. The source's labels load without their fields. */
        private @Nullable Expression container(NameId id) {
            Expression container = timeseries.get(id);
            if (container == null || isSource(container) == false) {
                return container;
            }
            var fields = new ArrayList<Expression>();
            for (String name : without.get(id)) {
                Attribute field = PromqlLabels.getLabel(source, name);
                // A label the source has no dimension for is in no container: leaving it out changes nothing.
                if (field != null) {
                    fields.add(field);
                }
            }
            return new TimeSeriesWithout(Source.EMPTY, fields, typed ? DataType.TIME_SERIES_METADATA_DATA_TYPE : DataType.KEYWORD);
        }

        /** The container {@code id} as a whole: the container with the promoted labels it is without written back. */
        private Expression whole(NameId id, Source at) {
            Expression whole = container(id);
            if (whole == null) {
                whole = typed ? TranslationContextV2.emptyLabels(at) : absent(at);
            }
            for (var label : promoted.getOrDefault(id, new TreeMap<>()).entrySet()) {
                if (inContainer(id, label.getKey()) == false) {
                    whole = new TimeSeriesMetadataSet(at, whole, Literal.keyword(at, label.getKey()), label.getValue());
                }
            }
            return whole;
        }

        /** Whether the container {@code id} holds {@code name}, promoted or not. */
        private boolean inContainer(NameId id, String name) {
            return timeseries.containsKey(id) && without.get(id).contains(name) == false;
        }

        /** The parts of the container {@code id} as attributes, the computed ones evaluated by {@code evals}. */
        private void materialize(NameId id, List<Alias> evals) {
            Expression container = container(id);
            if (container != null) {
                timeseries.put(id, attribute(container, MetadataAttribute.TIMESERIES, evals));
            }
            promoted.get(id).replaceAll((name, value) -> attribute(value, name, evals));
        }

        /** Drops the promoted labels of {@code id} that its container holds too and nothing reads by name. */
        private void dropCopies(NameId id) {
            promoted.get(id).keySet().removeIf(name -> inContainer(id, name) && need(id).contains(name) == false);
        }

        /** {@code expression} with every label read, and every container rendered or hashed, off the parts of its container. */
        private Expression scalar(Expression expression) {
            if (reads(expression) == false) {
                return expression;
            }
            return expression.transformUp(e -> switch (e) {
                case TimeSeriesMetadataPath path when reads(path.children().getFirst()) -> {
                    NameId id = record(path.children().getFirst());
                    String name = names(path.children().subList(1, 2)).getFirst();
                    yield label(timeseries.get(id), without.get(id), promoted.get(id), name, path.source());
                }
                case ToString render when render.field().dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(render.field()) -> {
                    Source at = render.source();
                    Expression whole = whole(record(render.field()), at);
                    yield typed ? new ToString(at, whole, configuration) : whole;
                }
                case HashOffset hash -> {
                    var keys = new ArrayList<Expression>();
                    for (Expression key : hash.children()) {
                        if (key.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(key)) {
                            // Each label once: a label in the container too would hash twice, so the same series would hash apart
                            // in plans that promote different labels.
                            NameId id = record(key);
                            if (timeseries.containsKey(id)) {
                                keys.add(container(id));
                            }
                            for (var label : promoted.get(id).entrySet()) {
                                if (inContainer(id, label.getKey()) == false) {
                                    keys.add(label.getValue());
                                }
                            }
                        } else {
                            keys.add(key);
                        }
                    }
                    yield keys.equals(hash.children()) ? hash : new HashOffset(hash.source(), keys);
                }
                default -> e;
            });
        }

        /** The container {@code id} without {@code out}, as the text a join compares; the empty container when nothing is left. */
        private Expression containerKey(NameId id, Set<String> out, Source at) {
            Expression container = unset(container(id), without.get(id), out, at);
            if (container == null) {
                container = typed ? TranslationContextV2.emptyLabels(at) : absent(at);
            }
            return new Coalesce(at, typed ? new ToString(at, container, configuration) : container, List.of(Literal.keyword(at, "")));
        }

    }
}
