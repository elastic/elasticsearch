/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeries;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlLabelRef;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Binds the label references ({@link UnresolvedPromqlLabelRef}) of a translated PromQL plan to columns, right after
 * {@link TranslatePromqlToEsqlPlan}, so translation never depends on which columns a plan carries.
 * <p>
 * Every metadata column is the source's {@code _timeseries} with some labels left out: operators only ever remove labels
 * from metadata and promote any label they add or rewrite, so the labels left out identify the column. Translation states
 * each operator's removals relative to its input ({@link UnresolvedPromqlLabelRef#unset()}). The rule works in two
 * directions. Bottom-up, it traces each metadata reference to the removals from the source and names it after them, e.g.
 * {@code _timeseries$dst$pod}. Top-down, it passes the labels each consumer needs toward its input, and binds every
 * reference on the way back: to the column its input carries, to a key carried up from the source, to metadata computed
 * from a finer view ({@link TimeSeriesUnset}) or read from it ({@link TimeSeriesGet}), or to null for a label no series
 * has. The source reads each view as keyword and casts it to typed metadata ({@link ToTimeSeries}); queries whose nodes
 * cannot read the typed form lower the result to keyword afterwards.
 */
public final class ResolvePromqlLabelRefs extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        Pass pass = new Pass(context);
        plan.forEachDown(EsRelation.class, relation -> relation.output().forEach(field -> {
            if (field.isDimension()) {
                pass.dimensions.add(PromqlLabels.labelName(field));
            }
        }));
        return pass.resolveTop(pass.follow(plan).plan());
    }

    /** {@code plan} with every metadata reference named after what it leaves out of the source's series, as binding sees it. */
    static LogicalPlan trace(LogicalPlan plan) {
        return new Pass(null).follow(plan).plan();
    }

    /** Stops at aggregate boundaries: functions inside an over-time aggregate do not survive as second-phase aggregates. */
    static boolean hasAcrossSeriesAggregate(Expression expression) {
        if (expression instanceof AggregateFunction) {
            return expression instanceof TimeSeriesAggregateFunction == false;
        }
        return expression.children().stream().anyMatch(ResolvePromqlLabelRefs::hasAcrossSeriesAggregate);
    }

    /** One resolution over the plan. */
    private static final class Pass {
        /** Null when the pass only traces metadata. */
        private final @Nullable AnalyzerContext context;
        /** Unset of each traced metadata column, against the source. */
        private final Map<String, Set<String>> unsetByName = new HashMap<>(Map.of(PromqlLabels.metadataLabelName(Set.of()), Set.of()));
        /**
         * Ids of the aliases translation puts over references: until a reference binds, its alias's {@link Alias#toAttribute()}
         * is an UnresolvedAttribute under the same id, a pin that must rebind. A user's own reference never has one of these ids.
         */
        private final Set<NameId> pinIds = new HashSet<>();
        /** The labels a source keeps as dimensions: only those can be in its metadata. */
        private final Set<String> dimensions = new HashSet<>();

        private Pass(@Nullable AnalyzerContext context) {
            this.context = context;
        }

        private final Map<NameId, UnresolvedPromqlLabelRef> followed = new HashMap<>();

        /** Followed plan plus the unset of its time-series metadata (composed toward the source). */
        private record Followed(LogicalPlan plan, @Nullable Set<String> unset) {
            Followed(LogicalPlan plan, @Nullable UnresolvedPromqlLabelRef timeSeriesMetadata) {
                this(plan, timeSeriesMetadata == null ? null : timeSeriesMetadata.unset());
            }
        }

        /**
         * Traces metadata bottom-up, consuming each metadata statement once, and collects local expressions of the
         * resulting node during the same traversal.
         */
        private Followed follow(LogicalPlan node) {
            Followed result = followNode(node);
            result.plan().forEachExpression(expression -> {
                if (expression instanceof UnresolvedPromqlLabelRef ref && ref.isMetadata()) {
                    unsetByName.putIfAbsent(ref.name(), ref.unset());
                }
                if (expression instanceof Alias alias && alias.child() instanceof UnresolvedPromqlLabelRef) {
                    pinIds.add(alias.id());
                }
            });
            return result;
        }

        private Followed followNode(LogicalPlan node) {
            List<Followed> children = node.children().stream().map(this::follow).toList();
            List<LogicalPlan> plans = children.stream().map(Followed::plan).toList();
            if (node instanceof UnionAll) {
                plans = nameBranchMetadata(plans);
            }
            LogicalPlan rebuilt = plans.equals(node.children()) ? node : node.replaceChildren(plans);
            // Union intersects unset so a name-dropping branch cannot strip labels the other still holds.
            Followed input;
            if (rebuilt instanceof UnionAll && children.isEmpty() == false) {
                input = unionMetadata(rebuilt, children);
            } else {
                input = children.isEmpty() ? null : children.getFirst();
            }
            Set<String> below = input == null || input.unset() == null ? Set.of() : input.unset();
            if (rebuilt instanceof Eval eval && eval.fields().stream().anyMatch(field -> isMetadata(field.child()))) {
                return followStatements(eval, below);
            }
            LogicalPlan plan = rebuilt.transformExpressionsOnly(
                UnresolvedPromqlLabelRef.class,
                ref -> ref.isMetadata() ? follow(ref, below) : ref
            );
            return metadataOf(plan, input);
        }

        /**
         * An Eval stating metadata: each statement leaves its labels out of the metadata before it, as a later field reads
         * the earlier ones, so the statements compose in field order and the last is the Eval's metadata. They go once
         * followed.
         */
        private Followed followStatements(Eval eval, Set<String> below) {
            Set<String> current = below;
            UnresolvedPromqlLabelRef stated = null;
            var fields = new ArrayList<Alias>();
            for (Alias field : eval.fields()) {
                if (isMetadata(field.child())) {
                    stated = follow((UnresolvedPromqlLabelRef) field.child(), current);
                    current = stated.unset();
                } else {
                    Set<String> input = current;
                    fields.add(
                        (Alias) field.transformDown(UnresolvedPromqlLabelRef.class, ref -> ref.isMetadata() ? follow(ref, input) : ref)
                    );
                }
            }
            LogicalPlan rest = fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
            return new Followed(rest, stated);
        }

        /**
         * Union branches project their metadata as {@code _timeseries}, though each holds the view it reads. When every branch
         * holds the same view, the branch columns take that view's name, so the union's consumer finds the view it reads by
         * name instead of deriving it again - which keyword metadata cannot. Branches holding different views keep the name:
         * the union then carries no single view.
         */
        private static List<LogicalPlan> nameBranchMetadata(List<LogicalPlan> branches) {
            String name = null;
            for (LogicalPlan branch : branches) {
                String branchName = branch instanceof Project project ? branchMetadataName(project) : null;
                if (branchName == null || (name != null && name.equals(branchName) == false)) {
                    return branches;
                }
                name = branchName;
            }
            if (name.equals(PromqlLabels.metadataLabelName(Set.of()))) {
                return branches;
            }
            String viewName = name;
            return branches.stream().<LogicalPlan>map(branch -> {
                Project project = (Project) branch;
                List<NamedExpression> projections = project.projections()
                    .stream()
                    .<NamedExpression>map(projection -> isBranchMetadata(projection) ? renamed((Alias) projection, viewName) : projection)
                    .toList();
                return project.withProjections(projections);
            }).toList();
        }

        /** {@code alias} under {@code name}, keeping its id so references to the branch column still bind. */
        private static Alias renamed(Alias alias, String name) {
            return new Alias(alias.source(), name, alias.child(), alias.id(), alias.synthetic());
        }

        /** The traced name of the view a branch projects as {@code _timeseries}, or null when it projects none. */
        private static @Nullable String branchMetadataName(Project branch) {
            for (NamedExpression projection : branch.projections()) {
                if (isBranchMetadata(projection)) {
                    return ((UnresolvedPromqlLabelRef) ((Alias) projection).child()).name();
                }
            }
            return null;
        }

        private static boolean isBranchMetadata(NamedExpression projection) {
            return projection instanceof Alias alias
                && alias.name().equals(PromqlLabels.metadataLabelName(Set.of()))
                && isMetadata(alias.child());
        }

        /** Intersect unset across branches; a metadata-less branch does not constrain the others. */
        private static Followed unionMetadata(LogicalPlan union, List<Followed> children) {
            Set<String> unset = null;
            for (Followed child : children) {
                if (child.unset() == null) {
                    continue;
                }
                if (unset == null) {
                    unset = new LinkedHashSet<>(child.unset());
                } else {
                    unset.retainAll(child.unset());
                }
            }
            return new Followed(union, unset);
        }

        /** Accumulate unset from source through ref. */
        private UnresolvedPromqlLabelRef follow(UnresolvedPromqlLabelRef ref, Set<String> below) {
            return followed.computeIfAbsent(ref.id(), id -> {
                var unset = new LinkedHashSet<>(below);
                unset.addAll(ref.unset());
                return ref.againstSource(unset);
            });
        }

        /**
         * What the metadata of {@code plan} leaves out of the source's series: an aggregate's is its key, or its input's when
         * it keeps one row per series; a projection's is the one it projects; any other operator passes its input's. Null
         * when the plan has none, its rows grouped by promoted labels only.
         */
        private static Followed metadataOf(LogicalPlan plan, @Nullable Followed input) {
            Followed passed = input == null ? new Followed(plan, Set.of()) : new Followed(plan, input.unset());
            if (plan instanceof Aggregate aggregate) {
                UnresolvedPromqlLabelRef key = metadataAmong(aggregate.groupings());
                if (key == null && aggregate.child() instanceof PackDims pack) {
                    key = metadataAmong(pack.dims());
                }
                if (key != null) {
                    return new Followed(plan, key);
                }
                boolean perSeries = aggregate instanceof TimeSeriesAggregate
                    && aggregate.aggregates().getFirst() instanceof Alias value
                    && ResolvePromqlLabelRefs.hasAcrossSeriesAggregate(value.child()) == false;
                return perSeries ? passed : new Followed(plan, (Set<String>) null);
            }
            if (plan instanceof Project project) {
                return new Followed(plan, metadataAmong(project.projections()));
            }
            return passed;
        }

        private static @Nullable UnresolvedPromqlLabelRef metadataAmong(List<? extends Expression> expressions) {
            for (Expression expression : expressions) {
                Expression read = expression instanceof Alias alias ? alias.child() : expression;
                if (read instanceof UnresolvedPromqlLabelRef ref && ref.isMetadata()) {
                    return ref;
                }
            }
            return null;
        }

        private static boolean isMetadata(Expression expression) {
            return expression instanceof UnresolvedPromqlLabelRef ref && ref.isMetadata();
        }

        /**
         * Tops of translated commands start with no label needs. Also resolve nodes that still hold unresolved
         * Alias pins (or-match TopNBy keys) so Binding can rematch them after the child resolves.
         */
        private LogicalPlan resolveTop(LogicalPlan node) {
            if (references(node).isEmpty() == false || hasUnresolvedAttributePins(node)) {
                return resolve(node, List.of());
            }
            List<LogicalPlan> children = node.children().stream().map(this::resolveTop).toList();
            return children.equals(node.children()) ? node : node.replaceChildren(children);
        }

        /**
         * Whether {@code node} reads a pin ({@code _timeseries}, {@code $_or_match}) left by an alias over a reference. A user's
         * own {@code _timeseries} reference ({@code DROP _timeseries}) or a {@code TIMESERIES_WITHOUT} column is ordinary
         * analysis input: matching pins by id keeps this rule off them.
         */
        private boolean hasUnresolvedAttributePins(LogicalPlan node) {
            Holder<Boolean> found = new Holder<>(false);
            node.forEachExpression(UnresolvedAttribute.class, unresolved -> {
                if (pinIds.contains(unresolved.id())) {
                    found.set(true);
                }
            });
            return found.get();
        }

        /** Rebuilds {@code node}, binding its own references, so that it carries every column of {@code needs} it can. */
        private LogicalPlan resolve(LogicalPlan node, List<UnresolvedPromqlLabelRef> needs) {
            if (isRaw(node)) {
                return node;
            }
            if (node instanceof Project project && project.child() instanceof UnpackDims unpack) {
                return resolveRegroup(project, unpack, needs);
            }
            if (node instanceof TimeSeriesAggregate collapse) {
                return resolveCollapse(collapse, needs);
            }
            if (node instanceof Aggregate aggregate) {
                return resolveAggregate(aggregate, needs);
            }
            if (node instanceof Project project) {
                return resolveProject(project, needs);
            }
            if (node instanceof HashJoin join) {
                return resolveJoin(join, needs);
            }
            if (node instanceof UnionAll union) {
                return resolveUnion(union, needs);
            }
            if (node instanceof UnaryPlan unary) {
                return resolveUnary(unary, needs);
            }
            throw new IllegalStateException("unexpected node in a translated PromQL plan [" + node.nodeName() + "]");
        }

        /** A plan below every aggregation: the source with its filters. Every label there is a dimension or absent. */
        private boolean isRaw(LogicalPlan node) {
            return node.anyMatch(p -> p instanceof Aggregate || p instanceof HashJoin) == false;
        }

        /**
         * Operators that pass their input's columns through: needs they do not define go to their input. An Eval that
         * derives a label drops the stored column it shadows.
         */
        private LogicalPlan resolveUnary(UnaryPlan node, List<UnresolvedPromqlLabelRef> needs) {
            Set<String> defined = new HashSet<>();
            if (node instanceof Eval eval) {
                eval.fields().forEach(field -> defined.add(PromqlLabels.labelName(field.toAttribute())));
            }
            List<UnresolvedPromqlLabelRef> passed = needs.stream().filter(need -> defined.contains(need.name()) == false).toList();
            List<UnresolvedPromqlLabelRef> own = references(node);
            // Own keys first: a PackDims match key must set the collapse grain before a coarser parent metadata need rides along.
            LogicalPlan child = resolve(node.child(), concat(own, passed));
            Binding binding = bind(node, child, own, List.of());
            LogicalPlan rebound = binding.apply(node.replaceChild(binding.plan()));
            return rebound instanceof Eval eval ? unshadow(eval) : rebound;
        }

        /**
         * {@code eval} without the stored labels it derives anew. A stored label under another column name (a passthrough
         * {@code labels.dst}) is not shadowed by the Eval's own {@code dst}, so a lookup by label name would still find it.
         */
        private LogicalPlan unshadow(Eval eval) {
            Set<String> derived = new HashSet<>();
            for (Alias field : eval.fields()) {
                if (field.child() instanceof Attribute == false && field.child() instanceof Literal == false) {
                    derived.add(field.name());
                }
            }
            List<Attribute> output = eval.output();
            List<Attribute> kept = output.stream()
                .filter(attribute -> derived.contains(attribute.name()) || derived.contains(PromqlLabels.labelName(attribute)) == false)
                .toList();
            return kept.size() < output.size() ? new Project(eval.source(), eval, kept) : eval;
        }

        /**
         * A projection states its columns: the labels it projects are references bound to its input, and no need passes it
         * from above - a vector match's result ends at the labels it projects. A reference aliased to a declared name and id
         * binds under that id; a label no series has is null under it.
         */
        private LogicalPlan resolveProject(Project node, List<UnresolvedPromqlLabelRef> needs) {
            List<UnresolvedPromqlLabelRef> own = references(node);
            var declared = new HashMap<NameId, NameId>();
            for (NamedExpression projection : node.projections()) {
                if (projection instanceof Alias alias && alias.child() instanceof UnresolvedPromqlLabelRef ref) {
                    declared.put(ref.id(), alias.id());
                }
            }
            Binding binding = bind(node, resolve(node.child(), own), own, List.of(), declared);
            var aliases = new ArrayList<Alias>();
            var projections = new ArrayList<NamedExpression>(node.projections().size());
            for (NamedExpression projection : node.projections()) {
                if (projection instanceof Alias exposed && exposed.child() instanceof UnresolvedPromqlLabelRef ref) {
                    Attribute column = binding.column(ref);
                    if (column.id().equals(exposed.id()) == false || column.name().equals(exposed.name()) == false) {
                        // Tracing may name the column _timeseries$...; rename to the declared public name for KEEP/API.
                        Alias alias = new Alias(column.source(), exposed.name(), column, exposed.id());
                        aliases.add(alias);
                        column = alias.toAttribute();
                    }
                    projections.add(column);
                } else {
                    projections.add(projection);
                }
            }
            LogicalPlan input = aliases.isEmpty() ? binding.plan() : new Eval(node.source(), binding.plan(), aliases);
            return binding.apply(new Project(node.source(), input, projections));
        }

        /**
         * The join reads its added fields from the build side; every other need goes to the side carrying it: the build side
         * for what it adds, else the probe side.
         */
        private LogicalPlan resolveJoin(HashJoin join, List<UnresolvedPromqlLabelRef> needs) {
            List<Attribute> buildOutput = join.right().output();
            var probe = new ArrayList<UnresolvedPromqlLabelRef>();
            var build = new ArrayList<UnresolvedPromqlLabelRef>();
            for (UnresolvedPromqlLabelRef need : needs) {
                (column(buildOutput, need) != null ? build : probe).add(need);
            }
            List<UnresolvedPromqlLabelRef> own = references(join);
            LogicalPlan left = resolve(join.left(), probe);
            LogicalPlan right = resolve(join.right(), concat(build, own));
            var added = new ArrayList<Attribute>();
            for (Attribute field : join.addedFields()) {
                Attribute column = field instanceof UnresolvedPromqlLabelRef ref ? column(right.output(), ref) : field;
                if (column != null) {
                    added.add(column);
                }
            }
            return new HashJoin(
                join.source(),
                left,
                right,
                join.leftFields(),
                join.rightFields(),
                added,
                join.unique(),
                join.config().type()
            );
        }

        /**
         * A need goes to every branch of a union that has the label: a branch reads the labels its type declares. The union's
         * columns are its branches' by name, a label a branch lacks being null there.
         */
        private LogicalPlan resolveUnion(UnionAll union, List<UnresolvedPromqlLabelRef> needs) {
            List<LogicalPlan> branches = union.children()
                .stream()
                .map(branch -> resolve(branch, needs.stream().filter(need -> column(branch.output(), need) != null).toList()))
                .toList();
            return new UnionAll(union.source(), branches, VectorBinarySet.unionOutputByName(branches));
        }

        /**
         * The initial aggregate collapses the source per series. A label it keys by or carries is a field column, dimension or
         * not: a non-dimension field is a label of the series that its metadata does not hold, so carrying it beside the
         * metadata keys by it too, as Prometheus keys by every label a WITHOUT leaves in. One the source lacks is dropped
         * from the keys. The source reads its metadata without the labels it leaves out.
         */
        private LogicalPlan resolveCollapse(TimeSeriesAggregate node, List<UnresolvedPromqlLabelRef> needs) {
            Alias value = (Alias) node.aggregates().getFirst();
            Grain grain = grain(node.groupings(), value);
            Keys keys = keys(grain, needs, true);
            LogicalPlan raw = node.child();
            LogicalPlan child = raw;
            Source source = node.source();

            var groupKeys = new ArrayList<NamedExpression>();
            for (UnresolvedPromqlLabelRef key : keys.metadata()) {
                List<Expression> fields = key.unset().stream().<Expression>map(label -> {
                    Attribute resolved = PromqlLabels.getLabel(raw.output(), label);
                    // TimeSeriesWithout reads the names of the labels it leaves out, present at the source or not
                    return resolved != null ? resolved : new ReferenceAttribute(Source.EMPTY, label, DataType.KEYWORD);
                }).toList();
                // The series read their metadata as a keyword view of the source, keyed by its typed form.
                groupKeys.add(new Alias(source, key.name(), new ToTimeSeries(source, new TimeSeriesWithout(source, fields))));
            }
            for (UnresolvedPromqlLabelRef label : keys.labels()) {
                Attribute column = PromqlLabels.getField(raw, label.name());
                if (column != null) {
                    groupKeys.add(column);
                }
            }
            var groupings = new ArrayList<Expression>();
            groupings.add(node.groupings().getFirst());
            groupings.addAll(groupKeys);
            List<NamedExpression> aggregates = placeKeys(node.aggregates(), groupKeys.stream().map(NamedExpression::toAttribute).toList());
            if (keys.declared()) {
                aggregates = wrapValues(aggregates);
            }
            return new TimeSeriesAggregate(source, child, groupings, aggregates, node.timeBucket(), node.timestamp(), node.origin());
        }

        /** A emitAggregate without packed dimensions: it keys by named labels only. */
        private LogicalPlan resolveAggregate(Aggregate node, List<UnresolvedPromqlLabelRef> needs) {
            Alias value = (Alias) node.aggregates().getFirst();
            Keys keys = keys(grain(node.groupings(), value), needs, false);
            List<UnresolvedPromqlLabelRef> own = references(node);
            LogicalPlan child = resolve(node.child(), concat(keys.pushed(), own));
            Binding binding = bind(node, child, own, keys.all());
            List<Attribute> columns = keys.all().stream().map(binding::column).toList();
            var groupings = new ArrayList<Expression>();
            groupings.add(node.groupings().getFirst());
            groupings.addAll(columns);
            return binding.apply(
                new Aggregate(node.source(), binding.plan(), groupings, wrapValues(placeKeys(node.aggregates(), columns)))
            );
        }

        /**
         * A emitAggregate over packed dimensions ({@link PackDims} under the {@link Aggregate}, {@link UnpackDims} and a projection
         * above it): the keys are the dimensions it packs. A label it cannot get from its input is absent from every series
         * and packs as null.
         */
        private LogicalPlan resolveRegroup(Project project, UnpackDims unpack, List<UnresolvedPromqlLabelRef> needs) {
            Aggregate aggregate = (Aggregate) unpack.child();
            PackDims pack = (PackDims) aggregate.child();
            Alias value = (Alias) aggregate.aggregates().getFirst();
            Keys keys = keys(grain(pack.dims(), value), needs, false);
            List<UnresolvedPromqlLabelRef> own = concat(references(pack), references(aggregate));
            LogicalPlan child = resolve(pack.child(), concat(keys.pushed(), own));
            Binding binding = bind(pack, child, own, keys.all());
            List<Attribute> dims = keys.all().stream().map(binding::column).toList();
            PackDims packed = new PackDims(pack.source(), binding.plan(), dims, pack.packed());
            Aggregate regrouped = (Aggregate) binding.apply(
                new Aggregate(aggregate.source(), packed, aggregate.groupings(), wrapValues(aggregate.aggregates()))
            );
            List<Attribute> unpacked = dims.stream()
                .<Attribute>map(
                    dim -> new ReferenceAttribute(
                        dim.source(),
                        null,
                        dim.name(),
                        dim.dataType().noText(),
                        Nullability.TRUE,
                        dim.id(),
                        false
                    )
                )
                .toList();
            UnpackDims unpackDims = new UnpackDims(unpack.source(), regrouped, unpack.packed(), unpacked);
            var projections = new ArrayList<NamedExpression>(project.projections().subList(0, 2));
            projections.addAll(unpacked);
            return new Project(project.source(), unpackDims, projections);
        }

        // -- keys --

        /**
         * What an aggregate keeps one row per: its metadata, which determines every label it does not leave out, and its
         * promoted keys, which determine only themselves. {@code perSeries} when its value aggregates nothing across series,
         * so each group is one input row.
         */
        private record Grain(@Nullable UnresolvedPromqlLabelRef metadata, List<UnresolvedPromqlLabelRef> labels, boolean perSeries) {}

        private Grain grain(List<? extends Expression> keyExpressions, Alias value) {
            UnresolvedPromqlLabelRef metadata = null;
            var labels = new ArrayList<UnresolvedPromqlLabelRef>();
            for (Expression key : keyExpressions) {
                if (key instanceof UnresolvedPromqlLabelRef ref) {
                    if (ref.isMetadata()) {
                        metadata = ref;
                    } else {
                        addIfAbsent(labels, ref);
                    }
                }
            }
            return new Grain(metadata, labels, hasAcrossSeriesAggregate(value.child()) == false);
        }

        /**
         * The keys of an aggregate: its metadata, then the labels pushed down through it, then its own labels.
         *
         * @param pushed what the aggregate needs of its input
         * @param declared whether the aggregate keys by anything, present at the source or not
         */
        private record Keys(
            List<UnresolvedPromqlLabelRef> metadata,
            List<UnresolvedPromqlLabelRef> labels,
            List<UnresolvedPromqlLabelRef> pushed,
            boolean declared
        ) {
            List<UnresolvedPromqlLabelRef> all() {
                return concat(metadata, labels);
            }
        }

        /**
         * A need rides along an aggregate when the aggregate's grain determines it. The initial aggregate of a series-preserving
         * operator keeps one row per series, which determines every label and all metadata; it reads one metadata at most.
         */
        private Keys keys(Grain grain, List<UnresolvedPromqlLabelRef> needs, boolean initial) {
            boolean perSeries = initial && grain.perSeries();
            var metadata = new ArrayList<UnresolvedPromqlLabelRef>();
            if (grain.metadata() != null) {
                metadata.add(grain.metadata());
            }
            // Whether anything above reads the aggregate's own metadata; one row per series keys by it for nothing else.
            boolean grainRead = grain.metadata() != null && needs.stream().anyMatch(grain.metadata()::reads);
            var carried = new ArrayList<UnresolvedPromqlLabelRef>();
            for (UnresolvedPromqlLabelRef need : needs) {
                if (need.isMetadata()) {
                    if (perSeries && metadata.stream().noneMatch(need::reads)) {
                        if (metadata.isEmpty()) {
                            metadata.add(need);
                        } else if (grainRead == false && metadata.size() == 1 && metadata.getFirst() == grain.metadata()) {
                            // Nothing reads the aggregate's own view: the source reads the one needed instead.
                            metadata.set(0, need);
                        } else {
                            // the source reads the finest metadata needed; a reader of a coarser one leaves labels out of it
                            int coarser = indexOf(metadata, read -> read.unset().containsAll(need.unset()));
                            if (coarser >= 0) {
                                metadata.set(coarser, need);
                            } else if (metadata.stream().noneMatch(read -> need.unset().containsAll(read.unset()))) {
                                // neither view derives from the other (a join key under ignoring(pod) and a group_left result
                                // leaving region out): the source reads what both leave out, and each reader derives its own
                                UnresolvedPromqlLabelRef read = metadata.getFirst();
                                var common = new LinkedHashSet<>(read.unset());
                                common.retainAll(need.unset());
                                UnresolvedPromqlLabelRef finest = read.againstSource(common);
                                unsetByName.putIfAbsent(finest.name(), finest.unset());
                                metadata.set(0, finest);
                            }
                        }
                    }
                } else if (grain.labels().stream().noneMatch(need::reads)
                    && (perSeries || (grain.metadata() != null && grain.metadata().unset().contains(need.name()) == false))) {
                        addIfAbsent(carried, need);
                    }
            }
            var labels = new ArrayList<>(carried);
            grain.labels().forEach(label -> addIfAbsent(labels, label));
            return new Keys(metadata, labels, concat(metadata, labels), metadata.isEmpty() == false || labels.isEmpty() == false);
        }

        private VerificationException unsupported(UnresolvedPromqlLabelRef ref) {
            return new VerificationException(
                List.of(Failure.fail(ref, "leaving labels out of _timeseries requires typed time-series metadata [{}]", ref.sourceText()))
            );
        }

        // -- binding --

        /** {@code plan} with null columns for the references it cannot bind, and the column each reference binds to. */
        private record Binding(LogicalPlan plan, Map<String, Attribute> byName) {
            /**
             * Rewrites {@code node}'s references into references to their columns. An attribute the node took from its input
             * while that input still read references is unresolved too; it now refers to the input's column with its id, or
             * with its name when it lost the id ({@link Alias#toAttribute()} of an unresolved alias).
             */
            LogicalPlan apply(LogicalPlan node) {
                var byId = new HashMap<NameId, Attribute>();
                var byExactName = new HashMap<String, Attribute>();
                plan.output().forEach(column -> {
                    byId.put(column.id(), column);
                    byExactName.put(column.name(), column);
                });
                return node.transformExpressionsOnly(Attribute.class, attribute -> {
                    Attribute column = attribute instanceof UnresolvedPromqlLabelRef ref ? byName.get(ref.name())
                        : attribute.resolved() ? null
                        : byId.getOrDefault(attribute.id(), byExactName.get(attribute.name()));
                    return column != null ? column : attribute;
                });
            }

            Attribute column(UnresolvedPromqlLabelRef ref) {
                return byName.get(ref.name());
            }
        }

        /**
         * Binds the references of {@code node} and the {@code keys} it groups by to columns of {@code child}. What the child
         * lacks is a null column defined right above it, under the reference's id where the node has one.
         */
        private Binding bind(LogicalPlan node, LogicalPlan child, List<UnresolvedPromqlLabelRef> own, List<UnresolvedPromqlLabelRef> keys) {
            return bind(node, child, own, keys, Map.of());
        }

        /** As above; a column the rule defines for a reference {@code declared} under another id takes that id. */
        private Binding bind(
            LogicalPlan node,
            LogicalPlan child,
            List<UnresolvedPromqlLabelRef> own,
            List<UnresolvedPromqlLabelRef> keys,
            Map<NameId, NameId> declared
        ) {
            var byName = new HashMap<String, Attribute>();
            var nulls = new ArrayList<Alias>();
            var ownByName = new HashMap<String, UnresolvedPromqlLabelRef>();
            own.forEach(ref -> ownByName.putIfAbsent(ref.name(), ref));
            for (UnresolvedPromqlLabelRef ref : concat(keys, own)) {
                if (byName.containsKey(ref.name())) {
                    continue;
                }
                Attribute column = column(child.output(), ref);
                if (column == null && ref.isMetadata()) {
                    // typed metadata the input carries a finer form of: leave the extra labels out with ts_unset
                    Attribute input = finerMetadata(child.output(), ref.unset());
                    if (input == null) {
                        throw unsupported(ref);
                    }
                    var labels = new LinkedHashSet<>(ref.unset());
                    labels.removeAll(unsetByName.get(input.name()));
                    UnresolvedPromqlLabelRef read = ownByName.get(ref.name());
                    Alias metadata = new Alias(
                        ref.source(),
                        ref.name(),
                        unset(ref.source(), input, labels),
                        read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId()
                    );
                    nulls.add(metadata);
                    column = metadata.toAttribute();
                }
                if (column == null && VectorBinaryOperator.EMPTY_SIGNATURE_KEY.equals(ref.name())) {
                    // the signature slot of an operand without metadata: the empty label set, which an opaque sibling's
                    // view equals once it has no label left outside the named key slots
                    UnresolvedPromqlLabelRef read = ownByName.get(ref.name());
                    NameId id = read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId();
                    Alias empty = new Alias(
                        ref.source(),
                        ref.name(),
                        new ToTimeSeries(ref.source(), Literal.keyword(ref.source(), "{}")),
                        id
                    );
                    nulls.add(empty);
                    column = empty.toAttribute();
                }
                if (column == null && ref.isMetadata() == false) {
                    // Opaque metadata holds the label: materialize it with ts_get (label_replace/join, on(...), by(...)).
                    // A label no source keeps as a dimension is in no metadata: no series has it.
                    Attribute metadata = dimensions.contains(PromqlLabels.labelName(ref.name()))
                        ? metadataKeeping(child.output(), ref.name())
                        : null;
                    if (metadata != null) {
                        UnresolvedPromqlLabelRef read = ownByName.get(ref.name());
                        NameId id = read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId();
                        Alias extracted = new Alias(
                            ref.source(),
                            ref.name(),
                            new TimeSeriesGet(ref.source(), metadata, Literal.keyword(ref.source(), ref.name())),
                            id
                        );
                        nulls.add(extracted);
                        column = extracted.toAttribute();
                    }
                }
                if (column == null) {
                    // under the id the node reads; a key pushed down from above gets its own, as each input defines it apart
                    UnresolvedPromqlLabelRef read = ownByName.get(ref.name());
                    NameId id = read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId();
                    Alias missing = new Alias(ref.source(), ref.name(), new Literal(ref.source(), null, DataType.KEYWORD), id);
                    nulls.add(missing);
                    column = missing.toAttribute();
                }
                byName.put(ref.name(), column);
            }
            LogicalPlan plan = nulls.isEmpty() ? child : new Eval(node.source(), child, nulls);
            return new Binding(plan, byName);
        }

        // -- helpers --

        /** The references {@code node} itself reads, in order. */
        private List<UnresolvedPromqlLabelRef> references(LogicalPlan node) {
            var own = new ArrayList<UnresolvedPromqlLabelRef>();
            node.forEachExpression(UnresolvedPromqlLabelRef.class, ref -> addIfAbsent(own, ref));
            return own;
        }

        /** The column of {@code output} carrying {@code ref}: a label by its canonical name, metadata by its exact name. */
        private @Nullable Attribute column(List<Attribute> output, UnresolvedPromqlLabelRef ref) {
            if (ref.isMetadata()) {
                for (Attribute attribute : output) {
                    if (attribute.name().equals(ref.name())) {
                        return attribute;
                    }
                }
                return null;
            }
            // A BY label named like the sample column is keyed as labels.<name> for Aggregate uniqueness; resolve by
            // canonical PromQL label name (strips the labels. prefix).
            return PromqlLabels.getLabel(output, PromqlLabels.labelName(ref.name()));
        }

        /**
         * The aggregates with their key references replaced by {@code keys}, placed where the first key reference stood,
         * else appended. Values keep their places: a shared aggregate of two operands lists both.
         */
        private List<NamedExpression> placeKeys(List<? extends NamedExpression> aggregates, List<Attribute> keys) {
            var placed = new ArrayList<NamedExpression>();
            boolean keysPlaced = false;
            for (NamedExpression aggregate : aggregates) {
                if (aggregate instanceof UnresolvedPromqlLabelRef) {
                    if (keysPlaced == false) {
                        placed.addAll(keys);
                        keysPlaced = true;
                    }
                } else {
                    placed.add(aggregate);
                }
            }
            if (keysPlaced == false) {
                placed.addAll(keys);
            }
            return placed;
        }

        /** {@code metadata} without {@code labels}. */
        private static Expression unset(Source source, Expression metadata, Set<String> labels) {
            return new TimeSeriesUnset(source, metadata, labels.stream().<Expression>map(label -> Literal.keyword(source, label)).toList());
        }

        /** Typed metadata column whose unset is the largest subset of desiredUnset. Null if none. */
        private @Nullable Attribute finerMetadata(List<Attribute> output, Set<String> desiredUnset) {
            Attribute finer = null;
            for (Attribute attribute : output) {
                Set<String> columnUnset = unsetByName.get(attribute.name());
                if (attribute.resolved()
                    && attribute.dataType() == DataType.TIMESERIES_DATA_TYPE
                    && columnUnset != null
                    && desiredUnset.containsAll(columnUnset)
                    && (finer == null || columnUnset.size() > unsetByName.get(finer.name()).size())) {
                    finer = attribute;
                }
            }
            return finer;
        }

        /** Finest typed metadata column that still holds label. Null if none. */
        private @Nullable Attribute metadataKeeping(List<Attribute> output, String label) {
            Attribute kept = null;
            for (Attribute attribute : output) {
                Set<String> columnUnset = unsetByName.get(attribute.name());
                if (attribute.resolved()
                    && attribute.dataType() == DataType.TIMESERIES_DATA_TYPE
                    && columnUnset != null
                    && columnUnset.contains(label) == false
                    && (kept == null || columnUnset.size() > unsetByName.get(kept.name()).size())) {
                    kept = attribute;
                }
            }
            return kept;
        }

        private static int indexOf(List<UnresolvedPromqlLabelRef> refs, Predicate<UnresolvedPromqlLabelRef> test) {
            for (int i = 0; i < refs.size(); i++) {
                if (test.test(refs.get(i))) {
                    return i;
                }
            }
            return -1;
        }

        /** Wraps every value that aggregates nothing across series in {@link Values}: each group is one input row. */
        private List<NamedExpression> wrapValues(List<? extends NamedExpression> aggregates) {
            var wrapped = new ArrayList<NamedExpression>(aggregates.size());
            for (NamedExpression aggregate : aggregates) {
                if (aggregate instanceof Alias value && hasAcrossSeriesAggregate(value.child()) == false) {
                    wrapped.add(value.replaceChild(new Values(value.child().source(), value.child())));
                } else {
                    wrapped.add(aggregate);
                }
            }
            return wrapped;
        }

        private static void addIfAbsent(List<UnresolvedPromqlLabelRef> refs, UnresolvedPromqlLabelRef ref) {
            if (refs.stream().noneMatch(ref::reads)) {
                refs.add(ref);
            }
        }

        private static List<UnresolvedPromqlLabelRef> concat(List<UnresolvedPromqlLabelRef> first, List<UnresolvedPromqlLabelRef> second) {
            var result = new ArrayList<UnresolvedPromqlLabelRef>(first);
            second.forEach(ref -> addIfAbsent(result, ref));
            return result;
        }
    }
}
