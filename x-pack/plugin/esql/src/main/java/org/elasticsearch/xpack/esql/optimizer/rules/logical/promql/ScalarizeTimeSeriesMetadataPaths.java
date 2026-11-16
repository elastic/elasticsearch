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
    public static boolean usesLabelContainer(AnalyzerContext context) {
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

        /** Step 2: what each container is in the rewritten plan. No entry: no node below defines it. */
        private final Map<NameId, Parts> containers = new HashMap<>();
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
                if (node instanceof UnionAll union) {
                    for (int i = 0; i < union.output().size(); i++) {
                        Attribute out = union.output().get(i);
                        for (LogicalPlan child : union.children()) {
                            Attribute branch = child.output().get(i);
                            if (isLabelContainer(branch)) {
                                List.copyOf(need(out.id())).forEach(label -> read(branch, label));
                                use(branch, tolerated.get(out.id()));
                            }
                        }
                    }
                    return;
                }
                // a key is compared: nothing of it may be missing
                List<Expression> keys = switch (node) {
                    case Aggregate aggregate -> aggregate.groupings();
                    case TopNBy topNBy -> topNBy.groupings();
                    case PackDims pack -> List.<Expression>copyOf(pack.dims());
                    case HashJoin join -> {
                        var both = new ArrayList<Expression>(join.leftFields());
                        both.addAll(join.rightFields());
                        yield both;
                    }
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

        // -- step 2: the rewrite. From the leaves up, each node on its own --

        /**
         * What a label container is in the rewritten plan, over the input of the node at hand: what is left of the container, and
         * the labels promoted out of it. A promoted label the container {@link #holds} is in both, alike. A label neither promoted
         * nor held is absent.
         */
        private static final class Parts {
            /** What is left of the container; null when nothing is. */
            private @Nullable Expression container;
            /** The labels the container is without: left out of it, written over outside of it, or never in it. */
            private final Set<String> without = new TreeSet<>();
            /** The labels promoted out of the container, each held by an expression of its own. */
            private SortedMap<String, Expression> labels = new TreeMap<>();

            private Parts(@Nullable Expression container) {
                this.container = container;
            }

            private boolean holds(String name) {
                return container != null && without.contains(name) == false;
            }

            /** The promoted labels the container does not hold. */
            private SortedMap<String, Expression> outside() {
                SortedMap<String, Expression> outside = new TreeMap<>(labels);
                outside.keySet().removeIf(this::holds);
                return outside;
            }
        }

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
            return switch (rewritten) {
                case UnionAll union -> rewriteUnionAll(union, children);
                case Aggregate aggregate -> rewriteAggregate(aggregate);
                case Eval eval -> rewriteEval(eval);
                case Project project -> rewriteProject(project);
                case HashJoin join -> rewriteHashJoin(join);
                case TopNBy topNBy -> rewriteTopNBy(topNBy);
                case PackDims pack -> rewritePackDims(pack);
                default -> rewritten.transformExpressionsOnlyUp(Expression.class, this::scalar);
            };
        }

        /**
         * The union with each label container position replaced by its parts. Every branch returns the same parts, which leave
         * the union under ids of their own: the analyzer pushes a conversion of a union output, such as the rendering of the
         * label container, into the branches, and would leave its attribute in the command's output.
         */
        private LogicalPlan rewriteUnionAll(UnionAll union, List<LogicalPlan> children) {
            var outputs = new UnionOutputs(children.size());
            // recorded once every position has read its branches: a union output has the id of its first branch
            var defined = new HashMap<NameId, Parts>();
            for (int i = 0; i < union.output().size(); i++) {
                Attribute out = union.output().get(i);
                if (isContainer(out) == false) {
                    outputs.passThrough(out, union.children(), i);
                    continue;
                }
                var branches = new ArrayList<Parts>();
                for (LogicalPlan child : union.children()) {
                    branches.add(containers.getOrDefault(child.output().get(i).id(), new Parts(null)));
                }
                defined.put(out.id(), unionParts(out, branches, outputs));
            }
            containers.putAll(defined);
            var branches = new ArrayList<LogicalPlan>();
            for (int j = 0; j < children.size(); j++) {
                LogicalPlan child = evaluate(children.get(j), outputs.evals.get(j));
                branches.add(new Project(child.source(), child, outputs.projections.get(j)));
            }
            return new Project(union.source(), new UnionAll(union.source(), branches, outputs.output), outputs.leaving);
        }

        /**
         * The parts of the union output {@code out}, a container position whose branches have {@code branches}. Where any branch
         * has a container, every branch returns its labels whole, and the union's container is without a label only where every
         * branch's is. Otherwise the branches line up by the labels any of them promotes. Outputs are named apart: the container's
         * own name for the container, prefixed for its labels.
         */
        private Parts unionParts(Attribute out, List<Parts> branches, UnionOutputs outputs) {
            Source at = out.source();
            Set<String> gone = null;
            var names = new TreeSet<String>();
            for (Parts branch : branches) {
                if (branch.container != null) {
                    if (gone == null) {
                        gone = new TreeSet<>(branch.without);
                    } else {
                        gone.retainAll(branch.without);
                    }
                }
                names.addAll(branch.labels.keySet());
            }
            var union = new Parts(null);
            if (gone != null) {
                List<Expression> whole = branches.stream().map(branch -> whole(branch, at)).toList();
                union.container = outputs.part(out.name(), typed ? DataType.TIME_SERIES_METADATA_DATA_TYPE : DataType.KEYWORD, whole);
                union.without.addAll(gone);
                union.without.removeAll(names);
                names.retainAll(need(out.id()));
            }
            for (String name : names) {
                List<Expression> values = branches.stream().map(branch -> label(branch, name, at)).toList();
                union.labels.put(name, outputs.part(out.name() + "." + name, DataType.KEYWORD, values));
            }
            return union;
        }

        /** The outputs of a union, what each branch projects to them, and what leaves the union for each. */
        private static final class UnionOutputs {
            private final List<List<Alias>> evals = new ArrayList<>();
            private final List<List<NamedExpression>> projections = new ArrayList<>();
            private final List<Attribute> output = new ArrayList<>();
            private final List<NamedExpression> leaving = new ArrayList<>();
            // The part of each list of branch attributes: a page holds each attribute once, so a branch projecting the same
            // attribute twice would leave the union an output short.
            private final Map<List<NameId>, Attribute> parts = new HashMap<>();

            UnionOutputs(int branches) {
                for (int j = 0; j < branches; j++) {
                    evals.add(new ArrayList<>());
                    projections.add(new ArrayList<>());
                }
            }

            void passThrough(Attribute out, List<LogicalPlan> branches, int position) {
                output.add(out);
                leaving.add(out);
                for (int j = 0; j < branches.size(); j++) {
                    projections.get(j).add(branches.get(j).output().get(position));
                }
            }

            /** The part named {@code name} that takes from each branch its value in {@code values}, as it leaves the union. */
            Attribute part(String name, DataType type, List<Expression> values) {
                var attributes = new ArrayList<Attribute>();
                for (int j = 0; j < values.size(); j++) {
                    attributes.add(attribute(values.get(j), name, evals.get(j)));
                }
                return parts.computeIfAbsent(attributes.stream().map(Attribute::id).toList(), key -> {
                    Attribute part = new ReferenceAttribute(values.getFirst().source(), null, name, type, Nullability.TRUE, null, false);
                    output.add(part);
                    for (int j = 0; j < attributes.size(); j++) {
                        Attribute attribute = attributes.get(j);
                        projections.get(j).add(attribute.name().equals(name) ? attribute : new Alias(attribute.source(), name, attribute));
                    }
                    Alias leaves = new Alias(part.source(), name, part);
                    leaving.add(leaves);
                    return leaves.toAttribute();
                });
            }
        }

        /**
         * The aggregate keyed by the parts of its label containers: each container, and its promoted labels. The time-series
         * aggregate keys by the labels as they are and its translation packs them; any other aggregate packs them itself: a label
         * of several values keys as a whole.
         */
        private LogicalPlan rewriteAggregate(Aggregate aggregate) {
            var evals = new ArrayList<Alias>();
            var groupings = new ArrayList<Expression>();
            var keys = new ArrayList<NameId>();
            var packedLabels = new ArrayList<Attribute>();
            source = promqlSource(aggregate);
            for (Expression grouping : aggregate.groupings()) {
                if ((grouping instanceof NamedExpression key && isContainer(key)) == false) {
                    groupings.add(grouping);
                    continue;
                }
                NamedExpression key = (NamedExpression) grouping;
                Parts parts = defineKey(key);
                if (aggregate instanceof TimeSeriesAggregate) {
                    groupings.addAll(loadKey(parts));
                } else {
                    materialize(parts, evals);
                    if (parts.container != null) {
                        groupings.add(parts.container);
                    }
                    parts.labels.values().forEach(label -> addIfAbsent(packedLabels, (Attribute) label));
                }
                keys.add(key.id());
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
                if (keys.contains(id)) {
                    Parts key = containers.get(id);
                    if (key.container != null) {
                        addIfAbsent(aggregates, (Attribute) key.container);
                    }
                    if (packedLabels.isEmpty()) {
                        key.labels.values().forEach(label -> addIfAbsent(aggregates, (Attribute) label));
                    }
                } else if (packedLabels.stream().noneMatch(label -> label.id().equals(id))) {
                    addIfAbsent(aggregates, (NamedExpression) scalar(aggregateExpression));
                }
            }
            if (packedLabels.isEmpty()) {
                return aggregate.with(child, groupings, aggregates);
            }
            return unpack(aggregate, child, groupings, aggregates, keys, packedLabels);
        }

        /**
         * Defines the container key {@code key} of an aggregate. The aggregate returns what it groups by: a container stays a key
         * of it, whether or not anything above uses it. A label every use above leaves out keys on its own, and the container is
         * without it: the key still tells apart what the container told apart. Of the source's labels only a dimension is in the
         * container.
         */
        private Parts defineKey(NamedExpression key) {
            Parts parts = define(key.id(), key instanceof Alias alias ? alias.child() : key);
            Set<String> out = tolerated.get(key.id());
            if (parts.container != null && out != null) {
                boolean loaded = isSource(parts.container);
                var split = new TreeSet<String>();
                for (String name : out) {
                    if (parts.without.contains(name) || loaded && PromqlLabels.getLabel(source, name) == null) {
                        continue;
                    }
                    Expression value = parts.labels.get(name);
                    if (value == null && loaded) {
                        value = PromqlLabels.getField(source, name);
                    }
                    if (value != null && isAbsent(value) == false) {
                        parts.labels.put(name, value);
                        split.add(name);
                    }
                }
                if (split.isEmpty() == false) {
                    unset(parts, split, Source.EMPTY);
                }
            }
            dropCopies(key.id(), parts);
            return parts;
        }

        /** The groupings of a time-series aggregate keyed by {@code key}: its parts as they are loaded. */
        private List<NamedExpression> loadKey(Parts key) {
            var aliases = new ArrayList<NamedExpression>();
            Expression container = container(key);
            if (container != null) {
                NamedExpression loaded = container instanceof Attribute attribute
                    ? attribute
                    : new Alias(container.source(), MetadataAttribute.TIMESERIES, container);
                aliases.add(loaded);
                if (isSource(key.container)) {
                    sources.put(loaded.id(), source);
                }
                key.container = loaded.toAttribute();
            }
            for (var label : key.labels.entrySet()) {
                // a source field keyed on gets an id of its own: above the aggregate it is a reference, not the field
                Alias keyed = new Alias(label.getValue().source(), label.getKey(), label.getValue());
                aliases.add(keyed);
                label.setValue(keyed.toAttribute());
            }
            return aliases;
        }

        /**
         * {@code aggregate} grouped by {@code groupings} and the {@code packedLabels} packed into one key, which is unpacked
         * above it. An unpacked label keeps its id: what reads it above the aggregate reads the unpacked attribute.
         */
        private LogicalPlan unpack(
            Aggregate aggregate,
            LogicalPlan child,
            List<Expression> groupings,
            List<NamedExpression> aggregates,
            List<NameId> keys,
            List<Attribute> packedLabels
        ) {
            Source at = aggregate.source();
            PackDims pack = new PackDims(at, child, packedLabels, PackDims.newPackedAttribute(at));
            Alias packed = PackDims.newPackedGrouping(at, pack.packed());
            groupings.add(packed);
            aggregates.add(packed.toAttribute());
            Aggregate packedAggregate = aggregate.with(pack, groupings, aggregates);
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
                Parts key = containers.get(id);
                if (key.container != null) {
                    addIfAbsent(output, (Attribute) key.container);
                }
                key.labels.replaceAll((name, label) -> unpacked.get(((Attribute) label).id()));
                key.labels.values().forEach(label -> addIfAbsent(output, (Attribute) label));
            }
            return new Project(at, new UnpackDims(at, packedAggregate, packed.toAttribute(), List.copyOf(unpacked.values())), output);
        }

        /**
         * Each part of a label container is evaluated once, here, where the container is defined: what names the container above
         * names its parts. A container that is an attribute already is not evaluated again.
         */
        private LogicalPlan rewriteEval(Eval eval) {
            var fields = new ArrayList<Alias>();
            for (Alias field : eval.fields()) {
                if (isContainer(field) == false) {
                    fields.add((Alias) scalar(field));
                    continue;
                }
                Parts parts = define(field.id(), field.child());
                if (parts.container != null && parts.container instanceof Attribute == false) {
                    Alias evaluated = new Alias(field.source(), field.name(), parts.container, field.id());
                    fields.add(evaluated);
                    parts.container = evaluated.toAttribute();
                }
                parts.labels.replaceAll((name, value) -> attribute(value, name, fields));
            }
            if (fields.equals(eval.fields())) {
                return eval;
            }
            return fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
        }

        /**
         * A projection keeps the parts of the containers it projects.
         */
        private LogicalPlan rewriteProject(Project project) {
            var projections = new ArrayList<NamedExpression>();
            for (NamedExpression projection : project.projections()) {
                if (isContainer(projection) == false) {
                    addIfAbsent(projections, (NamedExpression) scalar(projection));
                    continue;
                }
                Parts parts = record(projection.toAttribute());
                dropCopies(projection.id(), parts);
                if (parts.container != null) {
                    addIfAbsent(projections, (Attribute) parts.container);
                }
                parts.labels.replaceAll((name, label) -> {
                    Attribute attribute = (Attribute) label;
                    // The analyzer resolves a reference above by name: a label must not share the name of another projection.
                    boolean shadows = project.projections()
                        .stream()
                        .anyMatch(p -> p.name().equals(attribute.name()) && p.id().equals(attribute.id()) == false)
                        || projections.stream().anyMatch(p -> p.name().equals(attribute.name()) && p.id().equals(attribute.id()) == false);
                    NamedExpression projected = shadows
                        ? new Alias(attribute.source(), TemporaryNameGenerator.locallyUniqueTemporaryName(name), attribute)
                        : attribute;
                    addIfAbsent(projections, projected);
                    return projected.toAttribute();
                });
            }
            return new Project(project.source(), project.child(), projections);
        }

        /**
         * The join on the parts of its label container keys, packed into one key: each container whole where either side has
         * one, its labels by name otherwise.
         */
        private LogicalPlan rewriteHashJoin(HashJoin join) {
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
                Parts l = record(left);
                Parts r = record(right);
                if (l.container != null || r.container != null) {
                    // a container compares whole: its labels are what the other side holds in or outside its own
                    leftLabels.add(attribute(compared(l, left.source()), MetadataAttribute.TIMESERIES, leftEvals));
                    rightLabels.add(attribute(compared(r, right.source()), MetadataAttribute.TIMESERIES, rightEvals));
                    continue;
                }
                // no container: the labels compare by name, absent where a side has none
                var names = new TreeSet<>(l.labels.keySet());
                names.addAll(r.labels.keySet());
                for (String name : names) {
                    leftLabels.add(attribute(label(l, name, left.source()), name, leftEvals));
                    rightLabels.add(attribute(label(r, name, right.source()), name, rightEvals));
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
                Parts parts = record(attribute);
                dropCopies(attribute.id(), parts);
                Expression container = container(parts);
                if (container != null) {
                    Alias alias = new Alias(container.source(), attribute.name(), container);
                    projections.add(alias);
                    added.add(alias.toAttribute());
                    parts.container = alias.toAttribute();
                }
                parts.labels.replaceAll((name, label) -> {
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

        private LogicalPlan rewriteTopNBy(TopNBy topNBy) {
            var labels = new ArrayList<Attribute>();
            var groupings = new ArrayList<Expression>();
            for (Expression grouping : topNBy.groupings()) {
                if ((grouping instanceof Attribute attribute && isContainer(attribute)) == false) {
                    groupings.add(scalar(grouping));
                    continue;
                }
                Parts parts = record(grouping);
                if (parts.container != null) {
                    groupings.add(parts.container);
                }
                // a label the container holds too is compared with it
                parts.outside().values().forEach(label -> addIfAbsent(labels, (Attribute) label));
            }
            return new TopNBy(topNBy.source(), pack(topNBy.child(), labels, groupings), topNBy.order(), topNBy.limitPerGroup(), groupings);
        }

        /**
         * The pack of the parts of its label containers: the container that is left, and each label it does not hold.
         */
        private LogicalPlan rewritePackDims(PackDims pack) {
            var dims = new ArrayList<Attribute>();
            for (Attribute dim : pack.dims()) {
                if (isContainer(dim)) {
                    keys(record(dim)).forEach(part -> addIfAbsent(dims, (Attribute) part));
                } else {
                    dims.add(dim);
                }
            }
            return new PackDims(pack.source(), pack.child(), dims, pack.packed());
        }

        private boolean isContainer(NamedExpression named) {
            return containers.containsKey(named.id()) || isLabelContainer(named);
        }

        /** Whether {@code expression} reads a label container of the plan, or the source's labels within its aggregate. */
        private boolean reads(Expression expression) {
            return expression.anyMatch(e -> e instanceof Attribute attribute && isContainer(attribute))
                || (source != null && expression.anyMatch(ScalarizeTimeSeriesMetadataPaths::isSource));
        }

        /**
         * Records under {@code id} what the label container expression {@code definition} is. Starts from the container under
         * the {@code ts_*} calls of {@code definition} and applies the calls from that one up. Then promotes each label step 1
         * found read by name off {@code id}.
         */
        private Parts define(NameId id, Expression definition) {
            var calls = new ArrayDeque<Expression>();
            Expression under = definition;
            while (under instanceof TimeSeriesMetadataFilter || under instanceof TimeSeriesMetadataSet) {
                calls.push(under);
                under = under.children().getFirst();
            }
            Parts parts = start(under, definition);
            for (Expression call : calls) {
                switch (call) {
                    // TS_UNSET: neither the container nor a promoted label holds the names any more
                    case TimeSeriesMetadataFilter filter when filter.exclude() -> {
                        List<String> names = names(filter.labels());
                        unset(parts, names, call.source());
                        parts.labels.keySet().removeAll(names);
                    }
                    // TS_KEEP: the read of each name. Nothing is left of the container
                    case TimeSeriesMetadataFilter filter -> {
                        SortedMap<String, Expression> kept = new TreeMap<>();
                        for (String name : names(filter.labels())) {
                            Expression value = label(parts, name, call.source());
                            if (isAbsent(value) == false) {
                                kept.put(name, value);
                            }
                        }
                        parts.container = null;
                        parts.without.clear();
                        parts.labels = kept;
                    }
                    // TS_SET: the value is promoted, and the container is without the label. Writing null removes the label
                    case TimeSeriesMetadataSet set -> {
                        String name = names(List.of(set.label())).getFirst();
                        Expression value = emptyAsAbsent(scalar(set.value()));
                        unset(parts, List.of(name), call.source());
                        parts.labels.remove(name);
                        if (isAbsent(value) == false) {
                            parts.labels.put(name, value);
                        }
                    }
                    default -> throw new IllegalStateException("unexpected label container call [" + call + "]");
                }
            }
            for (String name : need(id)) {
                if (parts.labels.containsKey(name) == false) {
                    Expression value = label(parts, name, Source.EMPTY);
                    if (isAbsent(value) == false) {
                        parts.labels.put(name, value);
                    } else if (parts.container != null) {
                        // the source has no field for it: it is in no container
                        parts.without.add(name);
                    }
                }
            }
            containers.put(id, parts);
            return parts;
        }

        /** What the container {@code under}, at the bottom of the {@code ts_*} calls of {@code definition}, is. */
        private Parts start(Expression under, Expression definition) {
            if (under instanceof Attribute attribute && containers.containsKey(attribute.id())) {
                Parts below = containers.get(attribute.id());
                Parts parts = new Parts(below.container);
                parts.without.addAll(below.without);
                parts.labels.putAll(below.labels);
                return parts;
            }
            if (under instanceof Attribute attribute && isLabelContainer(attribute)) {
                // a container no node below defines: it is whole
                return new Parts(attribute);
            }
            if (isSource(under)) {
                if (source == null) {
                    throw new IllegalStateException("the source's label container is read outside of its aggregate");
                }
                // every label of the series; what it is without is left out where the aggregate loads it
                return new Parts(under);
            }
            if (under instanceof Literal) {
                // the empty container
                return new Parts(null);
            }
            throw new IllegalStateException("unexpected label container [" + definition + "]");
        }

        /**
         * The label {@code name} of {@code parts}: the expression it is promoted to, null when the container cannot hold it, or
         * the source's field where the aggregate at hand loads the container. A label is never read off a container: step 1 has
         * every label read by name promoted by the node that defines the container, so one that is not there cannot be read.
         */
        private Expression label(Parts parts, String name, Source at) {
            Expression value = parts.labels.get(name);
            if (value != null) {
                return value;
            }
            if (parts.container == null || parts.without.contains(name)) {
                return absent(at);
            }
            if (isSource(parts.container)) {
                Attribute field = PromqlLabels.getField(source, name);
                return field == null ? absent(at) : field;
            }
            throw new IllegalStateException("label [" + name + "] is read off [" + parts.container + "], which does not return it");
        }

        /**
         * Leaves {@code names} out of the container of {@code parts}. The source's labels leave them out where the aggregate loads
         * them. A container a time-series aggregate loads holds dimensions only, so it is without any other name already; where it
         * is keyword it cannot change, and a use that leaves out more reads it as it is, as the keyword translation did.
         */
        private void unset(Parts parts, Collection<String> names, Source at) {
            Expression container = parts.container;
            if (container != null && isSource(container) == false && parts.without.containsAll(names) == false) {
                var left = new LinkedHashSet<>(names);
                left.removeAll(parts.without);
                LogicalPlan series = base(container) instanceof Attribute loaded ? sources.get(loaded.id()) : null;
                if (series != null) {
                    left.removeIf(name -> PromqlLabels.getLabel(series, name) == null);
                }
                if (series == null || (left.isEmpty() == false && typed)) {
                    List<Expression> unset = left.stream().<Expression>map(name -> Literal.keyword(at, name)).toList();
                    parts.container = new TimeSeriesMetadataFilter(at, container, unset, true);
                }
            }
            parts.without.addAll(names);
        }

        /** What the label container {@code container} is: recorded for an attribute, defined anew for any other expression. */
        private Parts record(Expression container) {
            if (container instanceof Attribute attribute) {
                Parts parts = containers.get(attribute.id());
                return parts != null ? parts : define(attribute.id(), attribute);
            }
            return define(new NameId(), container);
        }

        /** The container of {@code parts}, null when nothing is left of it. The source's labels load without their fields. */
        private @Nullable Expression container(Parts parts) {
            if (parts.container == null || isSource(parts.container) == false) {
                return parts.container;
            }
            var fields = new ArrayList<Expression>();
            for (String name : parts.without) {
                Attribute field = PromqlLabels.getLabel(source, name);
                // A label the source has no dimension for is in no container: leaving it out changes nothing.
                if (field != null) {
                    fields.add(field);
                }
            }
            return new TimeSeriesWithout(Source.EMPTY, fields, typed ? DataType.TIME_SERIES_METADATA_DATA_TYPE : DataType.KEYWORD);
        }

        /** The container as a whole: the container with the promoted labels it does not hold written back. */
        private Expression whole(Parts parts, Source at) {
            Expression whole = container(parts);
            if (whole == null) {
                whole = typed ? TranslationContextV2.emptyLabels(at) : absent(at);
            }
            for (var label : parts.outside().entrySet()) {
                whole = new TimeSeriesMetadataSet(at, whole, Literal.keyword(at, label.getKey()), label.getValue());
            }
            return whole;
        }

        /**
         * The container as a join compares it: whole. A keyword container cannot be written to, and holds every label it has: a
         * side without one never matches a side with one.
         */
        private Expression compared(Parts parts, Source at) {
            if (typed) {
                return whole(parts, at);
            }
            Expression container = container(parts);
            return container == null ? absent(at) : container;
        }

        /** The parts that tell series apart: the container that is left, and each promoted label it does not hold. */
        private List<Expression> keys(Parts parts) {
            var keys = new ArrayList<Expression>();
            Expression container = container(parts);
            if (container != null) {
                keys.add(container);
            }
            keys.addAll(parts.outside().values());
            return keys;
        }

        /** The parts as attributes, the computed ones evaluated by {@code evals}. */
        private void materialize(Parts parts, List<Alias> evals) {
            Expression container = container(parts);
            if (container != null) {
                parts.container = attribute(container, MetadataAttribute.TIMESERIES, evals);
            }
            parts.labels.replaceAll((name, value) -> attribute(value, name, evals));
        }

        /** Drops the promoted labels of the container {@code id} that it holds too and nothing reads by name. */
        private void dropCopies(NameId id, Parts parts) {
            parts.labels.keySet().removeIf(name -> parts.holds(name) && need(id).contains(name) == false);
        }

        /** {@code expression} with every label read, and every container rendered or hashed, off the parts of its container. */
        private Expression scalar(Expression expression) {
            if (reads(expression) == false) {
                return expression;
            }
            return expression.transformUp(e -> switch (e) {
                case TimeSeriesMetadataPath path when reads(path.children().getFirst()) -> {
                    String name = names(path.children().subList(1, 2)).getFirst();
                    yield label(record(path.children().getFirst()), name, path.source());
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
                            keys.addAll(keys(record(key)));
                        } else {
                            keys.add(key);
                        }
                    }
                    yield keys.equals(hash.children()) ? hash : new HashOffset(hash.source(), keys);
                }
                default -> e;
            });
        }
    }
}
