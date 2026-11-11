/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
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
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Pushes the labels of a translated PromQL plan down into columns, right after {@link TranslatePromqlToEsqlPlan}.
 * <p>
 * Translation carries the labels of a series as one label container ({@code _timeseries}) and states every label operation as a
 * call over it: {@link TimeSeriesMetadataFilter} keeps some labels or leaves them out, {@link TimeSeriesMetadataSet} writes one
 * and {@link TimeSeriesMetadataPath} reads one; aggregations, joins, unions and hashing compare containers as a whole. Comparing and
 * rewriting whole containers is slow, and a node before 9.7 cannot read the typed container at all. This rule splits each container
 * into columns instead: a residual container, which the source reads as a view leaving out the labels the plan removes or writes,
 * and one column per label the plan names. Every operation then works on columns. Only the command's rendered label container
 * writes labels back into the residual one.
 * <p>
 * Where a node before 9.7 takes part ({@link PromqlCommand#usesLabelContainer}), the residual containers stay the keyword
 * {@code _timeseries} views of the source, and a plan that needs more of a container than that fails.
 * <p>
 * The containers a join or a union compares, and the ones an expression combines, are split alike: they share the set of labels in
 * columns. The rule first collects those sets ({@link Classes}), then rewrites the plan bottom-up, keeping each container's split as
 * a {@link Shape} until a node needs it as columns.
 * <p>
 * A collapse keeping one row per series keys by the series id when nothing above it reads a view of the series, and otherwise by the
 * one view the source reads: the one leaving out only what every reader leaves out. Any other aggregate keys by the view leaving out
 * every label its class removes or writes. Labels key packed ({@link PackDims}): a label of several values keys as a whole, and the
 * residual container, which is large, keys as it is.
 */
public final class PushDownPromqlLabels extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        boolean typed = PromqlCommand.usesLabelContainer(context);
        if (typed && context.configuration().pragmas().promqlLabelPushdown() == false) {
            // The translation is correct without it; a node before 9.7 reads only the keyword containers it leaves
            return plan;
        }
        return pushDown(plan, context.configuration(), typed);
    }

    static LogicalPlan pushDown(LogicalPlan plan, Configuration configuration) {
        return pushDown(plan, configuration, true);
    }

    /**
     * {@code plan} with its PromQL label containers split into columns; {@code configuration} renders the ones left whole. Without
     * {@code typed} a node before 9.7 takes part: the residual containers stay the keyword {@code _timeseries} views of the source,
     * and what has no keyword form fails: leaving labels out of a residual container after the collapse, reading or writing one.
     */
    static LogicalPlan pushDown(LogicalPlan plan, Configuration configuration, boolean typed) {
        Classes classes = Classes.of(plan);
        if (classes.isEmpty()) {
            return plan;
        }
        // A first run finds the views read above each collapse that keeps one row per series; the second one has it carry them.
        Map<NameId, Set<Set<String>>> views = new HashMap<>();
        new Pushdown(classes, configuration, typed, views, true).rewrite(plan);
        int found = views.values().stream().mapToInt(Set::size).sum();
        LogicalPlan rewritten = new Pushdown(classes, configuration, typed, views, false).rewrite(plan);
        if (views.values().stream().mapToInt(Set::size).sum() != found) {
            throw new IllegalStateException("label pushdown found views of a collapse after rewriting it");
        }
        if (typed == false) {
            // A keyword label container cannot leave labels out after the collapse, read or write one.
            rewritten.forEachExpressionDown(AbstractTimeSeriesMetadataFunction.class, function -> {
                throw new VerificationException(
                    List.of(
                        Failure.fail(
                            function,
                            "leaving labels out of _timeseries requires a typed label container [{}]",
                            function.sourceText()
                        )
                    )
                );
            });
        }
        return rewritten;
    }

    /** The source's label container, as translation reads it: every label of the series. */
    static boolean isSource(Expression expression) {
        return expression instanceof ToTimeSeriesMetadata cast
            && cast.field() instanceof TimeSeriesWithout without
            && without.children().isEmpty();
    }

    /** The label names a {@code ts_*} call lists after its container. */
    private static List<String> names(List<Expression> arguments) {
        return arguments.stream().map(argument -> BytesRefs.toString(((Literal) argument).value())).toList();
    }

    /**
     * Groups the label containers of a plan into classes of containers split alike: those derived from the same source series, those
     * an expression combines, and those a join or a union compares. Each class collects the labels its containers remove or write
     * and the labels they read.
     */
    static final class Classes {
        private final Map<NameId, Integer> byId = new HashMap<>();
        private final List<Integer> parents = new ArrayList<>();
        private final List<Set<String>> removed = new ArrayList<>();
        private final List<Set<String>> read = new ArrayList<>();

        static Classes of(LogicalPlan plan) {
            Classes classes = new Classes();
            plan.forEachUp(classes::visit);
            return classes;
        }

        boolean isEmpty() {
            return parents.isEmpty();
        }

        boolean contains(NameId id) {
            return byId.containsKey(id);
        }

        /** Whether {@code expression} reads a label container of the plan. */
        boolean reads(Expression expression) {
            return expression.anyMatch(e -> e instanceof Attribute attribute && contains(attribute.id()));
        }

        Set<String> removed(NameId id) {
            return removed.get(find(byId.get(id)));
        }

        /** Every label the containers of {@code id}'s class name: the ones they remove or write, and the ones they read. */
        Set<String> named(NameId id) {
            int root = find(byId.get(id));
            var named = new TreeSet<>(removed.get(root));
            named.addAll(read.get(root));
            return named;
        }

        private void visit(LogicalPlan node) {
            if (node instanceof UnionAll union) {
                for (int i = 0; i < union.output().size(); i++) {
                    var position = new ArrayList<Attribute>();
                    position.add(union.output().get(i));
                    for (LogicalPlan child : union.children()) {
                        position.add(child.output().get(i));
                    }
                    Integer merged = null;
                    for (Attribute attribute : position) {
                        merged = merge(merged, byId.get(attribute.id()));
                    }
                    if (merged != null) {
                        for (Attribute attribute : position) {
                            assign(attribute.id(), merged);
                        }
                    }
                }
            } else if (node instanceof HashJoin join) {
                for (int i = 0; i < join.leftFields().size(); i++) {
                    Attribute left = join.leftFields().get(i);
                    Attribute right = join.rightFields().get(i);
                    Integer merged = merge(byId.get(left.id()), byId.get(right.id()));
                    // Only PromQL joins on label containers; a side without labels joins on the empty one.
                    if (merged == null
                        && left.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE
                        && right.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE) {
                        merged = create();
                    }
                    if (merged != null) {
                        assign(left.id(), merged);
                        assign(right.id(), merged);
                    }
                }
            }
            // Only a PromQL collapse reads the source's labels as translation states them: other label containers are the user's.
            boolean promql = node instanceof TimeSeriesAggregate aggregate
                && aggregate.origin() == TimeSeriesAggregate.Origin.PROMQL_COMMAND;
            for (Expression expression : node.expressions()) {
                Expression body = expression instanceof Alias alias ? alias.child() : expression;
                Integer cls = classOf(body, promql);
                if (cls != null) {
                    if (expression instanceof Alias alias && alias.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE) {
                        assign(alias.id(), cls);
                    }
                    collect(body, cls);
                }
            }
        }

        private @Nullable Integer classOf(Expression expression, boolean promql) {
            var cls = new Integer[1];
            expression.forEachDown(e -> {
                if (promql && isSource(e)) {
                    cls[0] = merge(cls[0], create());
                } else if (e instanceof Attribute attribute) {
                    cls[0] = merge(cls[0], byId.get(attribute.id()));
                }
            });
            return cls[0];
        }

        private void collect(Expression expression, int cls) {
            int root = find(cls);
            expression.forEachDown(e -> {
                switch (e) {
                    case TimeSeriesMetadataFilter filter -> (filter.exclude() ? removed : read).get(root).addAll(names(filter.labels()));
                    case TimeSeriesMetadataSet set -> removed.get(root).addAll(names(List.of(set.label())));
                    case TimeSeriesMetadataPath get -> read.get(root).addAll(names(get.children().subList(1, 2)));
                    default -> {
                    }
                }
            });
        }

        private int create() {
            parents.add(parents.size());
            removed.add(new HashSet<>());
            read.add(new HashSet<>());
            return parents.size() - 1;
        }

        private int find(int cls) {
            while (parents.get(cls) != cls) {
                cls = parents.get(cls);
            }
            return cls;
        }

        private @Nullable Integer merge(@Nullable Integer left, @Nullable Integer right) {
            if (left == null) {
                return right;
            }
            return right == null ? left : union(left, right);
        }

        private int union(int left, int right) {
            int a = find(left);
            int b = find(right);
            if (a != b) {
                parents.set(b, a);
                removed.get(a).addAll(removed.get(b));
                read.get(a).addAll(read.get(b));
            }
            return a;
        }

        private void assign(NameId id, int cls) {
            byId.put(id, merge(byId.get(id), cls));
        }
    }

    /**
     * A label container split into columns: the residual container {@code rest}, which leaves {@code excluded} out, and the labels
     * in columns. A label in a column that the residual container does not leave out is in both, alike. A label in neither is absent.
     * Its parts are expressions over the input of the node at hand, until a node needs them as columns.
     */
    /**
     * A label container split into parts: the residual container and the labels in columns. A label in a column that the residual
     * container does not leave out is in both, alike. A label in neither is absent. The residual container is either an expression
     * ({@code rest}) or, above a collapse that keeps one row per series, a view of that collapse's source ({@code view}) leaving out
     * {@code excluded}: the collapse carries every view read above it. The parts are expressions over the input of the node at hand
     * until a node needs them as columns.
     */
    record Shape(@Nullable Expression rest, @Nullable NameId view, Set<String> excluded, SortedMap<String, Expression> labels) {

        static Shape empty() {
            return new Shape(null, null, Set.of(), new TreeMap<>());
        }

        boolean hasRest() {
            return rest != null || view != null;
        }

        Shape withLabels(SortedMap<String, Expression> columns) {
            return new Shape(rest, view, excluded, columns);
        }

        /** The residual container as the expression {@code rest}, rather than a view of a collapse. */
        Shape withRest(Expression rest) {
            return new Shape(rest, null, excluded, labels);
        }

        /** The labels alone: every other label is absent. */
        Shape withoutRest() {
            return new Shape(null, null, Set.of(), labels);
        }

        private static Set<String> union(Set<String> left, Collection<String> right) {
            var union = new TreeSet<>(left);
            union.addAll(right);
            return union;
        }
    }

    /** The join key comparing residual containers, as text. */
    private static final String REST_KEY = "$prom_rest";

    private static Literal absent(Source source) {
        return new Literal(source, null, DataType.KEYWORD);
    }

    private static boolean isAbsent(Expression expression) {
        return expression instanceof Literal literal && literal.value() == null;
    }

    /**
     * One rewrite of the plan, bottom-up. A first run only finds the views of each collapse that are read above it; the second run
     * has the collapses carry them.
     */
    private static final class Pushdown {
        private final Classes classes;
        private final Configuration configuration;
        /** The views read above each collapse that keeps one row per series, by the collapse's container grouping. */
        private final Map<NameId, Set<Set<String>>> views;
        /**
         * The view each such collapse carries: the source reads one view of its series, so the collapse carries the one leaving out
         * only what every reader leaves out, and the readers leave out the rest themselves.
         */
        private final Map<NameId, Shape> carried = new HashMap<>();
        /** The input of each collapse: where its views read their labels from. */
        private final Map<NameId, LogicalPlan> sources = new HashMap<>();
        /** The split of each label container, by the id of the column translation keeps it in. */
        private final Map<NameId, Shape> shapes = new HashMap<>();
        /** The input of the aggregate whose groupings are being split: where the source's labels are read from. */
        private LogicalPlan source;
        /** The container grouping being split: the source's labels belong to its class. */
        private NameId grouping;

        /** Whether this run only finds the views read above each collapse. */
        private final boolean finding;
        /** Whether every node reads the typed label container; otherwise the residual containers stay keyword. */
        private final boolean typed;

        private Pushdown(
            Classes classes,
            Configuration configuration,
            boolean typed,
            Map<NameId, Set<Set<String>>> views,
            boolean finding
        ) {
            this.classes = classes;
            this.configuration = configuration;
            this.typed = typed;
            this.views = views;
            this.finding = finding;
        }

        LogicalPlan rewrite(LogicalPlan node) {
            List<LogicalPlan> children = node.children().stream().map(this::rewrite).toList();
            if (node instanceof UnionAll union) {
                return union(union, children);
            }
            LogicalPlan rewritten = children.equals(node.children()) ? node : node.replaceChildrenSameSize(children);
            return switch (rewritten) {
                case Aggregate aggregate -> aggregate(aggregate);
                case Eval eval -> eval(eval);
                case Project project -> project(project);
                case HashJoin join -> join(join);
                case TopNBy topNBy -> topNBy(topNBy);
                default -> rewritten.transformExpressionsOnlyUp(Expression.class, this::scalar);
            };
        }

        private boolean isContainer(NamedExpression named) {
            return classes.contains(named.id());
        }

        /** Whether {@code expression} reads a label container of the plan, or the source's labels within its collapse. */
        private boolean reads(Expression expression) {
            return classes.reads(expression) || (source != null && expression.anyMatch(PushDownPromqlLabels::isSource));
        }

        // -- nodes --

        /**
         * The aggregate keyed by the parts of its label containers. A collapse keeping one row per series keys by every view read
         * above it and by the labels; any other aggregate keys by the residual container leaving out every label its class removes
         * or writes, so the parts above it need no residual container of their own. The time-series collapse keys by the labels as
         * they are and its translation packs them; any other aggregate packs them itself: a label of several values keys as a whole.
         */
        private LogicalPlan aggregate(Aggregate aggregate) {
            boolean collapse = aggregate instanceof TimeSeriesAggregate;
            LogicalPlan child = aggregate.child();
            var evals = new ArrayList<Alias>();
            var groupings = new ArrayList<Expression>();
            var keys = new HashMap<NameId, Shape>();
            var labels = new ArrayList<Attribute>();
            for (Expression grouping : aggregate.groupings()) {
                if (grouping instanceof NamedExpression named && isContainer(named)) {
                    source = collapse ? aggregate.child() : null;
                    this.grouping = named.id();
                    if (collapse) {
                        sources.put(named.id(), aggregate.child());
                    }
                    Expression definition = named instanceof Alias alias ? alias.child() : named;
                    Shape shape = shape(definition);
                    Shape columns;
                    if (collapse && isSource(definition)) {
                        if (finding == false && views.getOrDefault(named.id(), Set.of()).isEmpty()) {
                            // Nothing reads a view of the series above: the series id keys the collapse.
                            Attribute id = seriesId(aggregate.child());
                            child = withSeriesId(aggregate.child(), id);
                            groupings.add(id);
                            var aliases = new ArrayList<NamedExpression>();
                            columns = columns(shape.withLabels(shape.labels()).withoutRest(), aliases);
                            groupings.addAll(aliases);
                        } else {
                            columns = perSeries(named.id(), shape, groupings);
                        }
                    } else {
                        Shape key = finest(named.id(), shape);
                        var aliases = new ArrayList<NamedExpression>();
                        columns = collapse ? columns(key, aliases) : keyAttributes(key, evals);
                        if (collapse) {
                            groupings.addAll(aliases);
                        } else {
                            if (columns.rest() != null) {
                                groupings.add(columns.rest());
                            }
                            columns.labels().values().forEach(label -> addIfAbsent(labels, (Attribute) label));
                        }
                    }
                    keys.put(named.id(), columns);
                } else {
                    groupings.add(grouping);
                }
            }
            source = null;
            this.grouping = null;
            if (keys.isEmpty()) {
                return aggregate.transformExpressionsOnly(Expression.class, this::scalar);
            }
            child = withEvals(child, evals);
            if (labels.isEmpty()) {
                keys.forEach(shapes::put);
                return aggregate.with(child, groupings, aggregates(aggregate, keys, List.of()));
            }
            Source source = aggregate.source();
            PackDims pack = new PackDims(source, child, labels, PackDims.newPackedAttribute(source));
            Alias packed = PackDims.newPackedGrouping(source, pack.packed());
            groupings.add(packed);
            Aggregate packedAggregate = aggregate.with(pack, groupings, aggregates(aggregate, keys, List.of(packed.toAttribute())));
            var unpacked = new HashMap<NameId, Attribute>();
            for (Attribute label : labels) {
                unpacked.put(label.id(), column(label.source(), label.name(), label.dataType().noText()));
            }
            var output = new ArrayList<NamedExpression>();
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                Shape key = keys.get(aggregateExpression.id());
                if (key == null) {
                    output.add(aggregateExpression.toAttribute());
                    continue;
                }
                var columns = new TreeMap<String, Expression>();
                key.labels().forEach((name, label) -> columns.put(name, unpacked.get(((Attribute) label).id())));
                Shape unpackedKey = key.withLabels(columns);
                shapes.put(aggregateExpression.id(), unpackedKey);
                parts(unpackedKey).forEach(part -> addIfAbsent(output, (Attribute) part));
            }
            var dims = labels.stream().map(label -> unpacked.get(label.id())).toList();
            return new Project(source, new UnpackDims(source, packedAggregate, packed.toAttribute(), dims), output);
        }

        /**
         * The keys of a collapse keeping one row per series: its labels and the view of its source it carries. They determine the
         * series, as the view and the labels it leaves out do.
         */
        private Shape perSeries(NameId id, Shape shape, List<Expression> groupings) {
            var aliases = new ArrayList<NamedExpression>();
            Shape view = carried(id);
            Attribute column = (Attribute) view.rest();
            aliases.add(new Alias(column.source(), column.name(), sourceView(id, column.source(), view.excluded()), column.id()));
            var labels = new TreeMap<String, Expression>();
            shape.labels().forEach((name, value) -> labels.put(name, named(value, name, aliases)));
            groupings.addAll(aliases);
            return new Shape(null, id, shape.excluded(), labels);
        }

        /** The time-series id of the source under {@code plan}. */
        private static Attribute seriesId(LogicalPlan plan) {
            for (EsRelation relation : plan.collect(EsRelation.class)) {
                for (Attribute attribute : relation.output()) {
                    if (attribute.name().equals(MetadataAttribute.TSID_FIELD)) {
                        return attribute;
                    }
                }
            }
            return new MetadataAttribute(plan.source(), MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false);
        }

        /** {@code plan} with its source reading {@code id}. */
        private static LogicalPlan withSeriesId(LogicalPlan plan, Attribute id) {
            return plan.transformUp(
                EsRelation.class,
                relation -> relation.output().contains(id)
                    ? relation
                    : relation.withAttributes(CollectionUtils.combine(relation.output(), id))
            );
        }

        /** The view the collapse {@code id} carries, as its column and what it leaves out. */
        private Shape carried(NameId id) {
            return carried.computeIfAbsent(id, k -> {
                Set<String> excluded = null;
                for (Set<String> read : views.getOrDefault(id, Set.of())) {
                    if (excluded == null) {
                        excluded = new TreeSet<>(read);
                    } else {
                        excluded.retainAll(read);
                    }
                }
                Attribute column = column(Source.EMPTY, MetadataAttribute.TIMESERIES, containerType());
                return new Shape(column, null, excluded == null ? Set.of() : excluded, new TreeMap<>());
            });
        }

        /** {@code shape} as a key leaving out every label its class removes or writes, which its labels then hold. */
        private Shape finest(NameId id, Shape shape) {
            if (shape.hasRest() == false) {
                return shape;
            }
            Set<String> removed = classes.removed(id);
            var columns = new TreeMap<>(shape.labels());
            for (String label : removed) {
                if (columns.containsKey(label) == false && shape.excluded().contains(label) == false) {
                    columns.put(label, get(shape, Source.EMPTY, label));
                }
            }
            columns.values().removeIf(PushDownPromqlLabels::isAbsent);
            Set<String> excluded = Shape.union(shape.excluded(), removed);
            if (shape.view() != null) {
                return new Shape(null, shape.view(), excluded, columns);
            }
            if (shape.excluded().containsAll(removed)) {
                return shape.withLabels(columns);
            }
            return new Shape(restWithout(shape, Source.EMPTY, removed), null, excluded, columns);
        }

        /** The aggregates of {@code aggregate} with each label container key replaced by its parts, then {@code extra}. */
        private List<NamedExpression> aggregates(Aggregate aggregate, Map<NameId, Shape> keys, List<Attribute> extra) {
            var aggregates = new ArrayList<NamedExpression>();
            boolean packed = extra.isEmpty() == false;
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                Shape key = keys.get(aggregateExpression.id());
                if (key == null) {
                    aggregates.add((NamedExpression) scalar(aggregateExpression));
                    continue;
                }
                if (key.view() != null) {
                    addIfAbsent(aggregates, (Attribute) carried(key.view()).rest());
                } else if (key.rest() != null) {
                    addIfAbsent(aggregates, (Attribute) key.rest());
                }
                if (packed == false) {
                    key.labels().values().forEach(label -> addIfAbsent(aggregates, (Attribute) label));
                }
            }
            extra.forEach(attribute -> addIfAbsent(aggregates, attribute));
            return aggregates;
        }

        private LogicalPlan eval(Eval eval) {
            var fields = new ArrayList<Alias>();
            for (Alias field : eval.fields()) {
                if (isContainer(field)) {
                    // A keyword or keeps one series per label set on the label container as a whole: the verifier admits only
                    // the unions where that agrees with the match key, which leaves the metric name out.
                    Expression definition = typed == false && field.name().equals(VectorBinarySet.OR_MATCH_KEY)
                        ? field.child().children().getFirst()
                        : field.child();
                    shapes.put(field.id(), shape(definition));
                } else {
                    fields.add((Alias) scalar(field));
                }
            }
            if (fields.equals(eval.fields())) {
                return eval;
            }
            return fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
        }

        /** A projection keeps the parts of the containers it projects, and every view of a collapse a container still reads. */
        private LogicalPlan project(Project project) {
            var evals = new ArrayList<Alias>();
            var projections = new ArrayList<NamedExpression>();
            for (NamedExpression projection : project.projections()) {
                if (isContainer(projection)) {
                    Shape shape = projection instanceof Alias alias ? shape(alias.child()) : shapes.get(projection.id());
                    Shape columns = carried(shape, evals);
                    shapes.put(projection.id(), columns);
                    if (columns.view() != null) {
                        addIfAbsent(projections, (Attribute) carried(columns.view()).rest());
                    } else if (columns.rest() != null) {
                        addIfAbsent(projections, (Attribute) columns.rest());
                    }
                    columns.labels().values().forEach(part -> addIfAbsent(projections, (Attribute) part));
                } else {
                    projections.add((NamedExpression) scalar(projection));
                }
            }
            return new Project(project.source(), withEvals(project.child(), evals), projections);
        }

        /** The join on the parts of its label container keys: the residual containers as text, the labels packed. */
        private LogicalPlan join(HashJoin join) {
            var leftEvals = new ArrayList<Alias>();
            var rightEvals = new ArrayList<Alias>();
            var leftLabels = new ArrayList<Attribute>();
            var rightLabels = new ArrayList<Attribute>();
            var leftFields = new ArrayList<Attribute>();
            var rightFields = new ArrayList<Attribute>();
            for (int i = 0; i < join.leftFields().size(); i++) {
                Attribute left = join.leftFields().get(i);
                Attribute right = join.rightFields().get(i);
                if (isContainer(left) && isContainer(right)) {
                    Shape leftShape = shapes.get(left.id());
                    Shape rightShape = shapes.get(right.id());
                    var names = new TreeSet<>(leftShape.labels().keySet());
                    names.addAll(rightShape.labels().keySet());
                    for (String name : names) {
                        leftLabels.add(attribute(get(leftShape, left.source(), name), name, leftEvals));
                        rightLabels.add(attribute(get(rightShape, right.source(), name), name, rightEvals));
                    }
                    if (leftShape.hasRest() || rightShape.hasRest()) {
                        leftFields.add(attribute(restKey(left.source(), leftShape, names), REST_KEY, leftEvals));
                        rightFields.add(attribute(restKey(right.source(), rightShape, names), REST_KEY, rightEvals));
                    }
                } else {
                    leftFields.add(left);
                    rightFields.add(right);
                }
            }
            LogicalPlan right = packed(withEvals(join.right(), rightEvals), rightLabels, rightFields);
            // The join merges its output by name: the right side keeps its keys and what it adds, a container's parts under the
            // container's name, so they cannot shadow the left side's labels.
            var projections = new ArrayList<NamedExpression>(rightFields);
            var added = new ArrayList<Attribute>();
            for (Attribute attribute : join.addedFields()) {
                if (isContainer(attribute)) {
                    Shape shape = shapes.get(attribute.id());
                    Expression rest = shape.hasRest() ? renamed(rest(shape), attribute.name(), projections, added) : null;
                    var labels = new TreeMap<String, Expression>();
                    shape.labels()
                        .forEach((name, label) -> labels.put(name, renamed(label, attribute.name() + "." + name, projections, added)));
                    shapes.put(attribute.id(), new Shape(rest, null, shape.excluded(), labels));
                } else {
                    projections.add(attribute);
                    added.add(attribute);
                }
            }
            return new HashJoin(
                join.source(),
                packed(withEvals(join.left(), leftEvals), leftLabels, leftFields),
                new Project(right.source(), right, projections),
                leftFields,
                rightFields,
                added,
                join.unique(),
                join.config().type()
            );
        }

        private static Attribute renamed(Expression part, String name, List<NamedExpression> projections, List<Attribute> added) {
            Alias alias = new Alias(part.source(), name, part);
            projections.add(alias);
            added.add(alias.toAttribute());
            return alias.toAttribute();
        }

        /** {@code child} with {@code labels} packed into one key, which {@code keys} gets; a label of several values keys as a whole. */
        private static LogicalPlan packed(LogicalPlan child, List<Attribute> labels, List<? super Attribute> keys) {
            if (labels.isEmpty()) {
                return child;
            }
            Attribute packed = new ReferenceAttribute(child.source(), null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
            keys.add(packed);
            return new PackDims(child.source(), child, labels, packed);
        }

        private Expression restKey(Source source, Shape shape, Collection<String> slotted) {
            Expression rest = shape.hasRest() ? keyed(rest(shape), slotted) : emptyContainer(source);
            return new Coalesce(source, render(source, rest), List.of(Literal.keyword(source, "")));
        }

        private LogicalPlan union(UnionAll union, List<LogicalPlan> children) {
            var evals = new ArrayList<List<Alias>>();
            var projections = new ArrayList<List<NamedExpression>>();
            for (int j = 0; j < children.size(); j++) {
                evals.add(new ArrayList<>());
                projections.add(new ArrayList<>());
            }
            var output = new ArrayList<Attribute>();
            // The union output of each list of branch columns: a page holds each column once, so a branch projecting the same
            // column twice would leave the union a column short.
            var outputs = new HashMap<List<NameId>, Attribute>();
            for (int i = 0; i < union.output().size(); i++) {
                Attribute out = union.output().get(i);
                if (isContainer(out) == false) {
                    output.add(out);
                    for (int j = 0; j < children.size(); j++) {
                        projections.get(j).add(union.children().get(j).output().get(i));
                    }
                    continue;
                }
                var branches = new ArrayList<Shape>();
                for (int j = 0; j < children.size(); j++) {
                    Attribute branch = union.children().get(j).output().get(i);
                    Shape shape = shapes.get(branch.id());
                    branches.add(attributes(shape != null ? shape : Shape.empty(), evals.get(j)));
                }
                boolean rest = branches.stream().anyMatch(Shape::hasRest);
                var names = new TreeSet<String>();
                var excluded = new TreeSet<String>();
                branches.forEach(shape -> {
                    names.addAll(shape.labels().keySet());
                    excluded.addAll(shape.excluded());
                });
                Source source = out.source();
                Attribute restOut = null;
                // Union outputs are named apart: the container's own name for its residual part, prefixed for its labels.
                if (rest) {
                    var parts = new ArrayList<Expression>();
                    for (Shape branch : branches) {
                        parts.add(branch.rest() != null ? branch.rest() : cast(source, absent(source)));
                    }
                    restOut = unionOutput(parts, out.name(), containerType(), evals, projections, output, outputs);
                }
                var labels = new TreeMap<String, Expression>();
                for (String name : names) {
                    var parts = new ArrayList<Expression>();
                    for (Shape branch : branches) {
                        parts.add(get(branch, source, name));
                    }
                    labels.put(name, unionOutput(parts, out.name() + "." + name, DataType.KEYWORD, evals, projections, output, outputs));
                }
                shapes.put(out.id(), new Shape(restOut, null, excluded, labels));
            }
            var branches = new ArrayList<LogicalPlan>();
            for (int j = 0; j < children.size(); j++) {
                LogicalPlan child = withEvals(children.get(j), evals.get(j));
                branches.add(new Project(child.source(), child, projections.get(j)));
            }
            UnionAll rewritten = new UnionAll(union.source(), branches, output);
            // The parts leave the union under ids of their own: the analyzer pushes a conversion of a union output, such as the
            // rendering of the label container, into the branches, and would leave its column in the command's output.
            var renamed = new HashMap<NameId, Attribute>();
            var leaving = new ArrayList<NamedExpression>();
            for (Attribute column : output) {
                if (outputs.containsValue(column)) {
                    Alias alias = new Alias(column.source(), column.name(), column);
                    renamed.put(column.id(), alias.toAttribute());
                    leaving.add(alias);
                } else {
                    leaving.add(column);
                }
            }
            for (Attribute out : union.output()) {
                Shape shape = shapes.get(out.id());
                if (isContainer(out) && shape != null) {
                    Expression rest = shape.rest() == null ? null : renamed.get(((Attribute) shape.rest()).id());
                    var labels = new TreeMap<String, Expression>();
                    shape.labels().forEach((name, label) -> labels.put(name, renamed.get(((Attribute) label).id())));
                    shapes.put(out.id(), new Shape(rest, null, shape.excluded(), labels));
                }
            }
            return new Project(union.source(), rewritten, leaving);
        }

        /** The union output fed by {@code parts}, one per branch: the one already fed by the same columns, or a new one. */
        private static Attribute unionOutput(
            List<Expression> parts,
            String name,
            DataType type,
            List<List<Alias>> evals,
            List<List<NamedExpression>> projections,
            List<Attribute> output,
            Map<List<NameId>, Attribute> outputs
        ) {
            var columns = new ArrayList<Attribute>();
            for (int j = 0; j < parts.size(); j++) {
                columns.add(attribute(parts.get(j), name, evals.get(j)));
            }
            List<NameId> key = columns.stream().map(Attribute::id).toList();
            Attribute existing = outputs.get(key);
            if (existing != null) {
                return existing;
            }
            Attribute out = column(parts.getFirst().source(), name, type);
            output.add(out);
            outputs.put(key, out);
            for (int j = 0; j < columns.size(); j++) {
                Attribute column = columns.get(j);
                projections.get(j).add(column.name().equals(name) ? column : new Alias(column.source(), name, column));
            }
            return out;
        }

        private static Attribute column(Source source, String name, DataType type) {
            return new ReferenceAttribute(source, null, name, type, Nullability.TRUE, null, false);
        }

        private LogicalPlan topNBy(TopNBy topNBy) {
            var evals = new ArrayList<Alias>();
            var labels = new ArrayList<Attribute>();
            var groupings = new ArrayList<Expression>();
            for (Expression grouping : topNBy.groupings()) {
                if (grouping instanceof Attribute attribute && isContainer(attribute)) {
                    Shape columns = keyAttributes(shapes.get(attribute.id()), evals);
                    shapes.put(attribute.id(), columns);
                    if (columns.rest() != null) {
                        groupings.add(columns.rest());
                    }
                    columns.labels().values().forEach(label -> addIfAbsent(labels, (Attribute) label));
                } else {
                    groupings.add(scalar(grouping));
                }
            }
            return new TopNBy(
                topNBy.source(),
                packed(withEvals(topNBy.child(), evals), labels, groupings),
                topNBy.order(),
                topNBy.limitPerGroup(),
                groupings
            );
        }

        private static LogicalPlan withEvals(LogicalPlan child, List<Alias> evals) {
            return evals.isEmpty() ? child : new Eval(child.source(), child, evals);
        }

        // -- shapes --

        /** The split of a label container expression. */
        private Shape shape(Expression expression) {
            Source source = expression.source();
            return switch (expression) {
                case Attribute attribute when shapes.containsKey(attribute.id()) -> shapes.get(attribute.id());
                case Expression e when isSource(e) -> sourceShape();
                case ToTimeSeriesMetadata cast when cast.field() instanceof Literal -> Shape.empty();
                case Literal literal when literal.value() == null -> Shape.empty();
                case TimeSeriesMetadataFilter filter -> filter.exclude()
                    ? without(shape(filter.children().getFirst()), source, names(filter.labels()))
                    : keep(shape(filter.children().getFirst()), source, names(filter.labels()));
                case TimeSeriesMetadataSet set -> with(
                    shape(set.children().getFirst()),
                    source,
                    names(List.of(set.label())).getFirst(),
                    emptyAsAbsent(scalar(set.value()))
                );
                default -> throw new IllegalStateException("unexpected label container [" + expression + "]");
            };
        }

        /** The source's labels: a view of the collapse at hand, and a column for every label the class names. */
        private Shape sourceShape() {
            if (source == null) {
                throw new IllegalStateException("the source's label container is read outside of its aggregate");
            }
            var labels = new TreeMap<String, Expression>();
            var missing = new TreeSet<String>();
            for (String label : classes.named(grouping)) {
                Attribute field = PromqlLabels.getField(source, label);
                if (field != null) {
                    labels.put(label, field);
                } else {
                    missing.add(label);
                }
            }
            // A label the source has no field for is absent: leaving it out of the view changes nothing.
            return new Shape(null, grouping, missing, labels);
        }

        /** The view of the collapse {@code id}'s source leaving out {@code excluded}, as it reads it. */
        private Expression sourceView(NameId id, Source source, Set<String> excluded) {
            LogicalPlan input = sources.get(id);
            var without = new ArrayList<Expression>();
            for (String label : excluded) {
                Attribute field = PromqlLabels.getLabel(input, label);
                // TimeSeriesWithout reads the names of the labels it leaves out, present at the source or not
                without.add(field != null ? field : new ReferenceAttribute(Source.EMPTY, label, DataType.KEYWORD));
            }
            return cast(source, new TimeSeriesWithout(source, without));
        }

        /** The residual container of {@code shape} as an expression. */
        private Expression rest(Shape shape) {
            if (shape.view() == null) {
                return shape.rest();
            }
            // The collapse at hand reads its own views: they are its keys, not carried yet.
            if (shape.view().equals(grouping) && source != null) {
                return sourceView(shape.view(), Source.EMPTY, shape.excluded());
            }
            views.computeIfAbsent(shape.view(), k -> new HashSet<>()).add(new TreeSet<>(shape.excluded()));
            if (finding) {
                return column(Source.EMPTY, MetadataAttribute.TIMESERIES, containerType());
            }
            Shape view = carried(shape.view());
            var left = new TreeSet<>(shape.excluded());
            left.removeAll(view.excluded());
            return left.isEmpty()
                ? view.rest()
                : new TimeSeriesMetadataFilter(
                    Source.EMPTY,
                    view.rest(),
                    left.stream().<Expression>map(l -> Literal.keyword(Source.EMPTY, l)).toList(),
                    true
                );
        }

        private Expression get(Shape shape, Source source, String label) {
            Expression column = shape.labels().get(label);
            if (column != null) {
                return column;
            }
            if (shape.hasRest() == false || shape.excluded().contains(label)) {
                return absent(source);
            }
            // A label read without being named: it stays in the residual container.
            return new TimeSeriesMetadataPath(source, rest(shape), Literal.keyword(source, label));
        }

        private Shape without(Shape shape, Source source, Collection<String> names) {
            var columns = new TreeMap<>(shape.labels());
            names.forEach(columns::remove);
            Set<String> excluded = Shape.union(shape.excluded(), names);
            if (shape.view() != null) {
                return new Shape(null, shape.view(), excluded, columns);
            }
            return new Shape(restWithout(shape, source, names), null, excluded, columns);
        }

        private Shape keep(Shape shape, Source source, Collection<String> names) {
            var columns = new TreeMap<String, Expression>();
            for (String name : names) {
                Expression value = get(shape, source, name);
                if (isAbsent(value) == false) {
                    columns.put(name, value);
                }
            }
            return new Shape(null, null, Set.of(), columns);
        }

        private Shape with(Shape shape, Source source, String name, Expression value) {
            Shape without = without(shape, source, List.of(name));
            if (isAbsent(value) == false) {
                var columns = new TreeMap<>(without.labels());
                columns.put(name, value);
                return without.withLabels(columns);
            }
            return without;
        }

        private @Nullable Expression restWithout(Shape shape, Source source, Collection<String> names) {
            if (shape.rest() == null || shape.excluded().containsAll(names)) {
                return shape.rest();
            }
            var left = new LinkedHashSet<>(names);
            left.removeAll(shape.excluded());
            // Labels removed without being named: the residual container drops them itself.
            return new TimeSeriesMetadataFilter(
                source,
                shape.rest(),
                left.stream().<Expression>map(name -> Literal.keyword(source, name)).toList(),
                true
            );
        }

        /** The parts in a fixed order: the residual container, if any, then the labels by name. */
        private List<Expression> parts(Shape shape) {
            var parts = new ArrayList<Expression>();
            if (shape.hasRest()) {
                parts.add(rest(shape));
            }
            parts.addAll(shape.labels().values());
            return parts;
        }

        /**
         * The parts that make up the container, each label once: the residual container and the labels it leaves out. A label also in
         * the residual container would hash twice, so the same series would hash apart in plans that name different labels.
         */
        private List<Expression> identity(Shape shape) {
            var parts = new ArrayList<Expression>();
            if (shape.hasRest()) {
                parts.add(rest(shape));
            }
            shape.labels().forEach((name, label) -> {
                if (shape.hasRest() == false || shape.excluded().contains(name)) {
                    parts.add(label);
                }
            });
            return parts;
        }

        /** The container as a whole: the residual one with the labels it leaves out written back. */
        private Expression container(Shape shape, Source source) {
            Expression container = shape.hasRest() ? rest(shape) : emptyContainer(source);
            for (var label : shape.labels().entrySet()) {
                if (shape.hasRest() == false || shape.excluded().contains(label.getKey())) {
                    container = new TimeSeriesMetadataSet(source, container, Literal.keyword(source, label.getKey()), label.getValue());
                }
            }
            return container;
        }

        // -- keyword: a node before 9.7 reads _timeseries as keyword, so a residual container stays the source's keyword view --

        /**
         * The residual container {@code rest} keying beside the labels {@code slotted} in columns. A keyword one cannot leave
         * labels out after the collapse, so it compares those labels in the container too, as keyword vector matching always has.
         */
        private Expression keyed(Expression rest, Collection<String> slotted) {
            if (typed || (rest instanceof TimeSeriesMetadataFilter filter && filter.exclude()) == false) {
                return rest;
            }
            TimeSeriesMetadataFilter filter = (TimeSeriesMetadataFilter) rest;
            List<Expression> left = filter.labels()
                .stream()
                .filter(label -> slotted.contains(names(List.of(label)).getFirst()) == false)
                .toList();
            return left.isEmpty()
                ? filter.children().getFirst()
                : new TimeSeriesMetadataFilter(filter.source(), filter.children().getFirst(), left, true);
        }

        private DataType containerType() {
            return typed ? DataType.TIME_SERIES_METADATA_DATA_TYPE : DataType.KEYWORD;
        }

        private Expression cast(Source source, Expression keyword) {
            return typed ? new ToTimeSeriesMetadata(source, keyword) : keyword;
        }

        private Expression emptyContainer(Source source) {
            return typed ? new ToTimeSeriesMetadata(source, Literal.keyword(source, "{}")) : new Literal(source, null, DataType.KEYWORD);
        }

        private Expression render(Source source, Expression container) {
            return typed ? new ToString(source, container, configuration) : container;
        }

        /** A label value written as empty removes the label, as in Prometheus. */
        private static Expression emptyAsAbsent(Expression value) {
            if (value instanceof Literal literal) {
                return literal.value() == null || BytesRefs.toString(literal.value()).isEmpty() ? absent(value.source()) : value;
            }
            Source source = value.source();
            return new Case(source, new Equals(source, value, Literal.keyword(source, "")), List.of(absent(source), value));
        }

        /** {@code expression} with every label read and every container rendered or hashed off the split containers. */
        private Expression scalar(Expression expression) {
            if (reads(expression) == false) {
                return expression;
            }
            return expression.transformUp(e -> switch (e) {
                case TimeSeriesMetadataPath get when reads(get.children().getFirst()) -> get(
                    shape(get.children().getFirst()),
                    get.source(),
                    names(get.children().subList(1, 2)).getFirst()
                );
                case ToString render when render.field().dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(render.field()) ->
                    render(render.source(), container(shape(render.field()), render.source()));
                case HashOffset hash -> {
                    var keys = new ArrayList<Expression>();
                    for (Expression key : hash.children()) {
                        if (key.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE && reads(key)) {
                            keys.addAll(identity(shape(key)));
                        } else {
                            keys.add(key);
                        }
                    }
                    yield keys.equals(hash.children()) ? hash : new HashOffset(hash.source(), keys);
                }
                default -> e;
            });
        }

        // -- columns --

        /** {@code shape} with its parts as columns: the aliases computing them are appended to {@code aliases}, columns stay. */
        private Shape columns(Shape shape, List<NamedExpression> aliases) {
            Expression rest = shape.hasRest() ? named(rest(shape), MetadataAttribute.TIMESERIES, aliases) : null;
            var labels = new TreeMap<String, Expression>();
            shape.labels().forEach((name, value) -> labels.put(name, named(value, name, aliases)));
            return new Shape(rest, null, shape.excluded(), labels);
        }

        private static Expression named(Expression part, String name, List<NamedExpression> named) {
            // A source field keyed on gets an id of its own: above the aggregate it is a column, not the field.
            NamedExpression expression = part instanceof Attribute attribute && attribute instanceof FieldAttribute == false
                ? attribute
                : new Alias(part.source(), name, part);
            addIfAbsent(named, expression);
            return expression.toAttribute();
        }

        /** {@code shape} with its parts as attributes, the computed ones evaluated by {@code evals}. */
        private Shape attributes(Shape shape, List<Alias> evals) {
            Expression rest = shape.hasRest() ? attribute(rest(shape), MetadataAttribute.TIMESERIES, evals) : null;
            var labels = new TreeMap<String, Expression>();
            shape.labels().forEach((name, value) -> labels.put(name, attribute(value, name, evals)));
            return new Shape(rest, null, shape.excluded(), labels);
        }

        /** {@code shape} as the attributes of a key: its labels in columns are compared in a keyword residual container too. */
        private Shape keyAttributes(Shape shape, List<Alias> evals) {
            return shape.hasRest()
                ? attributes(shape.withRest(keyed(rest(shape), shape.labels().keySet())), evals)
                : attributes(shape, evals);
        }

        /** {@code shape} with its labels as attributes; a view of a collapse stays one, its column carried along. */
        private Shape carried(Shape shape, List<Alias> evals) {
            if (shape.view() == null) {
                return attributes(shape, evals);
            }
            rest(shape);
            var labels = new TreeMap<String, Expression>();
            shape.labels().forEach((name, value) -> labels.put(name, attribute(value, name, evals)));
            return shape.withLabels(labels);
        }

        private static Attribute attribute(Expression part, String name, List<Alias> evals) {
            if (part instanceof Attribute attribute) {
                return attribute;
            }
            // An evaluated part must not shadow a column of the same name, such as the label's own or the carried view.
            Alias alias = new Alias(part.source(), Attribute.rawTemporaryName(name, "temp_name", new NameId().toString()), part);
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
    }
}
