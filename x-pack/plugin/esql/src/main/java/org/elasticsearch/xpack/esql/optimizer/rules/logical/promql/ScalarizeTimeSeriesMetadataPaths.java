/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
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
 *     <li>a grouping of the time-series aggregate, a {@link PackDims} or the output uses the container as a whole.</li>
 * </ul>
 * In this model a container holds every label of its series. What the source loads holds the dimensions only, and reading a
 * label out of it row by row hurts the vectorized operator pipeline. So this rule replaces every read by name with a reference
 * to an attribute loaded next to the container, a compiler optimization called scalar replacement of aggregates (SROA), and loads
 * the container without what nothing needs of it, or not at all. The plan is right only once no read by name of what the source
 * loads is left: a read the rule cannot replace fails the query.
 * <p>
 * Outside the time-series aggregate the translation keys a group, a join or a ranking by a container packed, so a label of
 * several values keys as a whole. The rule never packs: it substitutes what a pack reads and what an unpack returns. A typed
 * container is the same bytes for the same label set and one value per row, so a pack takes it whole, its promoted labels
 * written back, and a pack of one typed container is gone: the container keys as it is. A label read above an aggregate keyed
 * by it reads off the key, which holds every label of the group. A keyword container cannot be written to: a pack takes its
 * parts.
 * <p>
 * The rule knows the container by the calls above alone, not by how the translation uses them. It has two steps
 * ({@link Run}):
 * <ol>
 *     <li>From the root down, changing nothing: for each container, find the labels read by name, and the labels every use of
 *     the container as a whole leaves out.</li>
 *     <li>From the leaves up, each node on its own: a node that defines a container promotes the labels read by name to
 *     attributes next to it, and a node that names a container names the container that is left and its promoted labels.</li>
 * </ol>
 */
public final class ScalarizeTimeSeriesMetadataPaths extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    /**
     * {@code plan} with its label containers split into parts; the configuration of {@code context} renders the ones left whole.
     */
    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        // the translation it follows emits label containers only with V2
        if (context.configuration().pragmas().enablePromqlTranslationV2() == false) {
            return plan;
        }
        if (plan.anyMatch(ScalarizeTimeSeriesMetadataPaths::readsSource) == false) {
            return plan;
        }
        return new Run() {
            @Override
            DataType containerType() {
                return DataType.TIME_SERIES_METADATA_DATA_TYPE;
            }

            @Override
            boolean writesContainers() {
                return true;
            }

            @Override
            Expression empty(Source at) {
                return TranslationContextV2.emptyLabels(at);
            }

            @Override
            Expression render(Source at, Expression whole) {
                return new ToString(at, whole, context.configuration());
            }

            @Override
            boolean keysWhole() {
                return true;
            }

            @Override
            void verify(LogicalPlan rewritten) {
                // what is left is a typed container every node reads
            }
        }.apply(plan);
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
     * Fails where {@code node}, any node but the time-series aggregate, keys by a label container itself: the translation packs
     * every such key, so a label of several values keys as a whole, and the rule only substitutes what the pack reads.
     */
    private static void requirePacked(LogicalPlan node, List<? extends Expression> keys) {
        for (Expression key : keys) {
            if (isLabelContainer(key)) {
                throw new IllegalStateException("[" + node.nodeName() + "] keys by the label container [" + key + "] unpacked");
            }
        }
    }

    /**
     * One run of the rule over one plan. All state is kept by the id of a label container attribute of the plan: step 1 fills what
     * the plan asks of each container, step 2 what each container is in the rewritten plan. The hooks below are what a typed and a
     * keyword label container do differently: each run {@link #apply(LogicalPlan, AnalyzerContext)} makes overrides every one of
     * them.
     */
    abstract static class Run {
        /** Step 1: the labels read by name off each container. */
        private final Map<NameId, Set<String>> need = new HashMap<>();
        /** Step 1: the labels every use of each container as a whole leaves out. No entry: nothing uses it as a whole. */
        private final Map<NameId, Set<String>> tolerated = new HashMap<>();

        /** Step 2: what each container is in the rewritten plan. No entry: no node below defines it. */
        private final Map<NameId, Parts> containers = new HashMap<>();
        /** The input of the time-series aggregate that loads each of these container attributes. */
        private final Map<NameId, LogicalPlan> sources = new HashMap<>();
        /** Step 2: what a pack takes of each container it reads, in order: what an unpack above returns of the container. */
        private final Map<NameId, List<Attribute>> packed = new HashMap<>();
        /** Step 2: the typed container that keys as it is in place of a pack of it, by the id of what names the pack. */
        private final Map<NameId, Attribute> keyed = new HashMap<>();
        /** The input of the time-series aggregate whose groupings are being rewritten: where the source's labels are read. */
        private LogicalPlan source;

        // -- hooks: what differs between the typed and the keyword container --

        /** The type of a container the rewrite builds. */
        abstract DataType containerType();

        /** Whether a call can change a container after the source loads it. */
        abstract boolean writesContainers();

        /** The container of a series that has no labels. */
        abstract Expression empty(Source at);

        /** {@code whole}, a container as a whole, rendered as the string a query asks for. */
        abstract Expression render(Source at, Expression whole);

        /**
         * Whether a pack takes a container whole, the promoted labels it does not hold written back. A typed container is the same
         * bytes for the same label set and one value per row, so it keys as it is, and a label read above an aggregate keyed by
         * it reads off the key. A keyword container cannot be written to, so a pack takes its parts instead.
         */
        abstract boolean keysWhole();

        /** Checks the rewritten plan: what is left of it must run on every participating node. */
        abstract void verify(LogicalPlan rewritten);

        LogicalPlan apply(LogicalPlan plan) {
            demand(plan);
            LogicalPlan rewritten = rewrite(plan);
            verify(rewritten);
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
                switch (node) {
                    case TimeSeriesAggregate ignored -> {
                    }
                    case Aggregate aggregate -> requirePacked(aggregate, aggregate.groupings());
                    case TopNBy topNBy -> requirePacked(topNBy, topNBy.groupings());
                    case HashJoin join -> {
                        requirePacked(join, join.leftFields());
                        requirePacked(join, join.rightFields());
                    }
                    default -> {
                    }
                }
                // a key is compared: nothing of it may be missing
                List<Expression> keys = switch (node) {
                    case Aggregate aggregate -> aggregate.groupings();
                    case PackDims pack -> List.copyOf(pack.dims());
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
        static final class Parts {
            /** What is left of the container; null when nothing is. */
            private @Nullable Expression container;
            /** The labels the container is without: left out of it, written over outside of it, or never in it. */
            private final Set<String> without = new TreeSet<>();
            /** The labels promoted out of the container, each held by an expression of its own. */
            private SortedMap<String, Expression> labels = new TreeMap<>();
            /** Where nothing is left of the container after a TS_KEEP: every label it keeps, promoted or absent. Null otherwise. */
            private @Nullable SortedSet<String> kept;

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
            if (keyed.isEmpty() == false && (rewritten instanceof Aggregate || rewritten instanceof UnpackDims) == false) {
                // what names a pack that is gone names the container that keys in its place
                rewritten = rewritten.transformExpressionsOnly(Attribute.class, a -> keyed.getOrDefault(a.id(), a));
            }
            return switch (rewritten) {
                case UnionAll union -> rewriteUnionAll(union, children);
                case Aggregate aggregate -> rewriteAggregate(aggregate);
                case Eval eval -> rewriteEval(eval);
                case Project project -> rewriteProject(project);
                case HashJoin join -> rewriteHashJoin(join);
                case PackDims pack -> rewritePackDims(pack);
                case UnpackDims unpack -> rewriteUnpackDims(unpack);
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
                union.container = outputs.part(out.name(), containerType(), whole);
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
         * The time-series aggregate keyed by the parts of its label containers, as they are loaded: its translation packs them.
         * Any other aggregate keys by a container packed (see {@link #rewritePackDims}).
         */
        private LogicalPlan rewriteAggregate(Aggregate aggregate) {
            if (aggregate instanceof TimeSeriesAggregate == false) {
                return keyedInPlaceOfPacks(aggregate).transformExpressionsOnly(Expression.class, this::scalar);
            }
            var groupings = new ArrayList<Expression>();
            var keys = new ArrayList<NameId>();
            source = promqlSource(aggregate);
            for (Expression grouping : aggregate.groupings()) {
                if (grouping instanceof NamedExpression key && isContainer(key)) {
                    groupings.addAll(loadKey(defineKey(key)));
                    keys.add(key.id());
                } else {
                    groupings.add(grouping);
                }
            }
            source = null;
            if (keys.isEmpty()) {
                return aggregate.transformExpressionsOnly(Expression.class, this::scalar);
            }
            // the aggregates, with each container key replaced by its parts
            var aggregates = new ArrayList<NamedExpression>();
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                if (keys.contains(aggregateExpression.id())) {
                    Parts key = containers.get(aggregateExpression.id());
                    if (key.container != null) {
                        addIfAbsent(aggregates, (Attribute) key.container);
                    }
                    key.labels.values().forEach(label -> addIfAbsent(aggregates, (Attribute) label));
                } else {
                    addIfAbsent(aggregates, (NamedExpression) scalar(aggregateExpression));
                }
            }
            return aggregate.with(aggregate.child(), groupings, aggregates);
        }

        /**
         * {@code aggregate} keyed by the typed container that keys in place of a pack of it: the grouping that names the pack names
         * the container, and so does what reads the grouping above.
         */
        private Aggregate keyedInPlaceOfPacks(Aggregate aggregate) {
            var groupings = new ArrayList<Expression>();
            for (Expression grouping : aggregate.groupings()) {
                Attribute key = Alias.unwrap(grouping) instanceof Attribute pack ? keyed.get(pack.id()) : null;
                if (key == null) {
                    groupings.add(grouping);
                    continue;
                }
                keyed.put(((NamedExpression) grouping).id(), key);
                groupings.add(key);
            }
            if (groupings.equals(aggregate.groupings())) {
                return aggregate;
            }
            var aggregates = new ArrayList<NamedExpression>();
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                Attribute key = aggregateExpression instanceof Attribute attribute ? keyed.get(attribute.id()) : null;
                addIfAbsent(aggregates, key != null ? key : aggregateExpression);
            }
            return aggregate.with(aggregate.child(), groupings, aggregates);
        }

        /**
         * {@code plan} with the labels read by name off each container key in {@code keys} read off the key, which holds every
         * label of the group, above the aggregate.
         */
        private LogicalPlan readKeyLabels(LogicalPlan plan, List<NameId> keys) {
            var evals = new ArrayList<Alias>();
            for (NameId id : keys) {
                Parts key = containers.get(id);
                for (String name : need(id)) {
                    if (key.without.contains(name)) {
                        // the key is without it: it reads absent
                        continue;
                    }
                    Source at = key.container.source();
                    Expression read = new TimeSeriesMetadataPath(at, key.container, Literal.keyword(at, name));
                    key.labels.put(name, attribute(read, name, evals));
                }
            }
            return evaluate(plan, evals);
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
         * The join with what it adds of its label containers replaced by their parts: under the container's name, so they cannot
         * shadow the left side's labels, as the join merges its output by name. It keys by a container packed (see
         * {@link #rewritePackDims}).
         */
        private LogicalPlan rewriteHashJoin(HashJoin join) {
            if (join.addedFields().stream().noneMatch(this::isContainer)) {
                return join;
            }
            var projections = new ArrayList<NamedExpression>(join.rightFields());
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
                join.left(),
                new Project(join.right().source(), join.right(), projections),
                join.leftFields(),
                join.rightFields(),
                added,
                join.unique(),
                join.config().type()
            );
        }

        /**
         * The pack of the parts of each label container it reads. A typed container is packed whole, the promoted labels it does
         * not hold written back: the same label set is the same value wherever it is packed. A pack of one typed container is
         * gone: the container keys as it is. A keyword container cannot be written to: the container that is left is packed with
         * each promoted label, a label of several values as a whole, and nothing left of a kept container is packed with every
         * label kept, absent where a series has none. A container no part is left of packs as absent.
         */
        private LogicalPlan rewritePackDims(PackDims pack) {
            var evals = new ArrayList<Alias>();
            var dims = new ArrayList<Attribute>();
            for (Attribute dim : pack.dims()) {
                if (isContainer(dim) == false) {
                    addIfAbsent(dims, dim);
                    continue;
                }
                Parts parts = record(dim);
                var taken = new ArrayList<Attribute>();
                if (keysWhole()) {
                    taken.add(attribute(whole(parts, dim.source()), MetadataAttribute.TIMESERIES, evals));
                } else {
                    dropCopies(dim.id(), parts);
                    materialize(parts, evals);
                    if (parts.container != null) {
                        taken.add((Attribute) parts.container);
                    }
                    // What is left of a kept container is every label kept, absent where a series has none: two packs of the
                    // same labels kept take alike, whatever labels their sources have fields for.
                    var names = new TreeSet<>(parts.labels.keySet());
                    if (parts.container == null && parts.kept != null) {
                        names.addAll(parts.kept);
                    }
                    for (String name : names) {
                        Expression label = parts.labels.get(name);
                        taken.add(label != null ? (Attribute) label : attribute(absent(dim.source()), name, evals));
                    }
                    if (taken.isEmpty()) {
                        taken.add(attribute(absent(dim.source()), MetadataAttribute.TIMESERIES, evals));
                    }
                }
                packed.put(dim.id(), taken);
                taken.forEach(part -> addIfAbsent(dims, part));
            }
            if (keysWhole() && pack.dims().size() == 1 && packed.containsKey(pack.dims().getFirst().id())) {
                // One typed container is one value per row, the same for the same label set: it keys as it is, and what names
                // the pack names it instead.
                keyed.put(pack.packed().id(), dims.getFirst());
                return evaluate(pack.child(), evals);
            }
            return new PackDims(pack.source(), evaluate(pack.child(), evals), dims, pack.packed());
        }

        /**
         * The unpack of what {@link #rewritePackDims} packed of each label container: each part keeps its id, so the parts of the
         * container above are the parts below. A container packed whole is unpacked whole, and where its pack is gone the
         * aggregate returns it as it is: a label read off it above reads off the key, which holds every label of the group.
         */
        private LogicalPlan rewriteUnpackDims(UnpackDims unpack) {
            boolean gone = keyed.containsKey(unpack.packed().id());
            var dims = new ArrayList<Attribute>();
            var whole = new ArrayList<NameId>();
            for (Attribute dim : unpack.dims()) {
                List<Attribute> taken = packed.get(dim.id());
                if (taken == null) {
                    addIfAbsent(dims, dim);
                    continue;
                }
                var unpacked = new HashMap<NameId, Attribute>();
                for (Attribute part : taken) {
                    // where the pack is gone, the aggregate returns the part as it is
                    Attribute attribute = gone
                        ? part
                        : new ReferenceAttribute(
                            part.source(),
                            null,
                            part.name(),
                            part.dataType().noText(),
                            Nullability.TRUE,
                            part.id(),
                            false
                        );
                    unpacked.put(part.id(), attribute);
                    addIfAbsent(dims, attribute);
                }
                Parts key = containers.get(dim.id());
                if (keysWhole()) {
                    // the key holds the labels written back into it, and is without the others the container was without
                    key.container = unpacked.get(taken.getFirst().id());
                    key.without.removeAll(key.labels.keySet());
                    key.labels = new TreeMap<>();
                    whole.add(dim.id());
                } else {
                    if (key.container != null) {
                        key.container = unpacked.get(((Attribute) key.container).id());
                    }
                    key.labels.replaceAll((name, label) -> unpacked.get(((Attribute) label).id()));
                }
            }
            LogicalPlan plan = gone ? unpack.child() : new UnpackDims(unpack.source(), unpack.child(), unpack.packed(), dims);
            return readKeyLabels(plan, whole);
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
                        if (parts.kept != null) {
                            parts.kept.removeAll(names);
                        }
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
                        parts.kept = new TreeSet<>(names(filter.labels()));
                    }
                    // TS_SET: the value is promoted, and the container is without the label. Writing null removes the label
                    case TimeSeriesMetadataSet set -> {
                        String name = names(List.of(set.label())).getFirst();
                        Expression value = emptyAsAbsent(scalar(set.value()));
                        unset(parts, List.of(name), call.source());
                        parts.labels.remove(name);
                        if (parts.kept != null) {
                            parts.kept.add(name);
                        }
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
                parts.kept = below.kept == null ? null : new TreeSet<>(below.kept);
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
                if (series == null || (left.isEmpty() == false && writesContainers())) {
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
        @Nullable
        Expression container(Parts parts) {
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
            return new TimeSeriesWithout(Source.EMPTY, fields, containerType());
        }

        /** The container as a whole: the container with the promoted labels it does not hold written back. */
        Expression whole(Parts parts, Source at) {
            Expression whole = container(parts);
            if (whole == null) {
                whole = empty(at);
            }
            for (var label : parts.outside().entrySet()) {
                whole = new TimeSeriesMetadataSet(at, whole, Literal.keyword(at, label.getKey()), label.getValue());
            }
            return whole;
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
                    yield render(at, whole);
                }
                case HashOffset hash -> {
                    var keys = new ArrayList<Expression>();
                    for (Expression key : hash.children()) {
                        if (key.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(key) && keysWhole()) {
                            // the container whole: the same series hashes alike whatever labels a plan promotes
                            keys.add(whole(record(key), key.source()));
                        } else if (key.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(key)) {
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
