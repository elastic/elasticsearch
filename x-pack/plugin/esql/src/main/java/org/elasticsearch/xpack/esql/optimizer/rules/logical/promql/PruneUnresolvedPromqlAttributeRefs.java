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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFromKeyword;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlAttribute;
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
 * Binds the label references ({@link UnresolvedPromqlAttribute}) of a translated PromQL plan to columns, right after
 * {@link TranslatePromqlToEsqlPlan} and {@link PathTraceUnresolvedPromqlAttributeRefs}, so translation never depends on which columns a
 * plan carries. A reference binds to the
 * column its input carries, to a key carried up from the source, to metadata computed from a finer one (typed
 * {@link TimeSeriesUnset} only), or to null for a label no series has; anything else is not supported.
 */
public final class PruneUnresolvedPromqlAttributeRefs extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return new Pass(context, plan).resolveTop(plan);
    }

    /** Stops at aggregate boundaries: functions inside an over-time aggregate do not survive as second-phase aggregates. */
    static boolean hasAcrossSeriesAggregate(Expression expression) {
        if (expression instanceof AggregateFunction) {
            return expression instanceof TimeSeriesAggregateFunction == false;
        }
        return expression.children().stream().anyMatch(PruneUnresolvedPromqlAttributeRefs::hasAcrossSeriesAggregate);
    }

    /** One resolution over the plan. {@code typed} when every participating node reads {@code _timeseries} as typed metadata. */
    private static final class Pass {
        private final AnalyzerContext context;
        private final boolean typed;
        /** Unset of each PathTrace-named metadata column (against source). */
        private final Map<String, Set<String>> unsetByName = new HashMap<>(Map.of(PromqlLabels.metadataLabelName(Set.of()), Set.of()));

        private Pass(AnalyzerContext context, LogicalPlan plan) {
            this.context = context;
            this.typed = PromqlCommand.usesTimeSeriesMetadata(context);
            // NamePromqlMetadataReferences named every metadata reference after what it leaves out
            plan.forEachExpressionDown(UnresolvedPromqlAttribute.class, ref -> {
                if (ref.isMetadata()) {
                    unsetByName.putIfAbsent(ref.name(), ref.unset());
                }
            });
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
         * Alias.toAttribute of unresolved tsIdentity metadata leaves UnresolvedAttribute pins ({@code _timeseries},
         * {@code $_or_match}) that must rebind. Ordinary analysis UnresolvedAttributes must not trigger this path.
         */
        private static boolean hasUnresolvedAttributePins(LogicalPlan node) {
            boolean[] found = { false };
            node.transformExpressionsOnly(Attribute.class, attribute -> {
                if (attribute instanceof UnresolvedAttribute unresolved && isTsIdentityPin(unresolved.name())) {
                    found[0] = true;
                }
                return attribute;
            });
            return found[0];
        }

        private static boolean isTsIdentityPin(String name) {
            return name != null
                && (MetadataAttribute.TIMESERIES.equals(name)
                    || name.startsWith(MetadataAttribute.TIMESERIES + "$")
                    || VectorBinarySet.OR_MATCH_KEY.equals(name));
        }

        /** Rebuilds {@code node}, binding its own references, so that it carries every column of {@code needs} it can. */
        private LogicalPlan resolve(LogicalPlan node, List<UnresolvedPromqlAttribute> needs) {
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
        private LogicalPlan resolveUnary(UnaryPlan node, List<UnresolvedPromqlAttribute> needs) {
            Set<String> defined = new HashSet<>();
            if (node instanceof Eval eval) {
                eval.fields().forEach(field -> defined.add(PromqlLabels.labelName(field.toAttribute())));
            }
            List<UnresolvedPromqlAttribute> passed = needs.stream().filter(need -> defined.contains(need.name()) == false).toList();
            List<UnresolvedPromqlAttribute> own = references(node);
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
        private LogicalPlan resolveProject(Project node, List<UnresolvedPromqlAttribute> needs) {
            List<UnresolvedPromqlAttribute> own = references(node);
            var declared = new HashMap<NameId, NameId>();
            for (NamedExpression projection : node.projections()) {
                if (projection instanceof Alias alias && alias.child() instanceof UnresolvedPromqlAttribute ref) {
                    declared.put(ref.id(), alias.id());
                }
            }
            Binding binding = bind(node, resolve(node.child(), own), own, List.of(), declared);
            var aliases = new ArrayList<Alias>();
            var projections = new ArrayList<NamedExpression>(node.projections().size());
            for (NamedExpression projection : node.projections()) {
                if (projection instanceof Alias exposed && exposed.child() instanceof UnresolvedPromqlAttribute ref) {
                    Attribute column = binding.column(ref);
                    if (ref.isMetadata() && column.dataType() == DataType.KEYWORD && PromqlCommand.timeseriesType() != DataType.KEYWORD) {
                        // a keyword read where the command exposes typed metadata
                        Alias alias = new Alias(
                            ref.source(),
                            exposed.name(),
                            new TimeSeriesFromKeyword(ref.source(), column),
                            exposed.id()
                        );
                        aliases.add(alias);
                        column = alias.toAttribute();
                    } else if (column.id().equals(exposed.id()) == false || column.name().equals(exposed.name()) == false) {
                        // PathTrace may name the column _timeseries$…; rename to the declared public name for KEEP/API.
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
        private LogicalPlan resolveJoin(HashJoin join, List<UnresolvedPromqlAttribute> needs) {
            List<Attribute> buildOutput = join.right().output();
            var probe = new ArrayList<UnresolvedPromqlAttribute>();
            var build = new ArrayList<UnresolvedPromqlAttribute>();
            for (UnresolvedPromqlAttribute need : needs) {
                (column(buildOutput, need) != null ? build : probe).add(need);
            }
            List<UnresolvedPromqlAttribute> own = references(join);
            LogicalPlan left = resolve(join.left(), probe);
            LogicalPlan right = resolve(join.right(), concat(build, own));
            var added = new ArrayList<Attribute>();
            for (Attribute field : join.addedFields()) {
                Attribute column = field instanceof UnresolvedPromqlAttribute ref ? column(right.output(), ref) : field;
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
        private LogicalPlan resolveUnion(UnionAll union, List<UnresolvedPromqlAttribute> needs) {
            List<LogicalPlan> branches = union.children()
                .stream()
                .map(branch -> resolve(branch, needs.stream().filter(need -> column(branch.output(), need) != null).toList()))
                .toList();
            return new UnionAll(union.source(), branches, VectorBinarySet.unionOutputByName(branches));
        }

        /**
         * The initial aggregate collapses the source per series. A label it keys by, or can push down, is a dimension column;
         * one the source lacks is dropped from the keys. The source reads its metadata without the labels it leaves out.
         */
        private LogicalPlan resolveCollapse(TimeSeriesAggregate node, List<UnresolvedPromqlAttribute> needs) {
            Alias value = (Alias) node.aggregates().getFirst();
            Keys keys = keys(grain(node.groupings(), value), needs, true);
            LogicalPlan raw = node.child();
            LogicalPlan child = raw;
            Source source = node.source();

            var groupKeys = new ArrayList<NamedExpression>();
            if (typed && keys.metadata().isEmpty() == false) {
                // The source reads typed metadata leaving its labels out, as it reads the keyword column a
                // TimeSeriesWithout lowers to: the loader resolves the label names against the mapping either way.
                var metadata = new ArrayList<TimeSeriesMetadataAttribute>();
                for (UnresolvedPromqlAttribute key : keys.metadata()) {
                    var read = TimeSeriesMetadataAttribute.create(source, key.unset(), context.minimumVersion());
                    metadata.add(read);
                    groupKeys.add(key.unset().isEmpty() ? read : new Alias(source, key.name(), read));
                }
                child = child.transformUp(EsRelation.class, relation -> {
                    var attributes = new ArrayList<>(relation.output());
                    attributes.removeIf(attribute -> attribute instanceof TimeSeriesMetadataAttribute);
                    attributes.addAll(metadata);
                    return relation.withAttributes(attributes);
                });
            } else {
                for (UnresolvedPromqlAttribute key : keys.metadata()) {
                    List<Expression> fields = key.unset().stream().<Expression>map(label -> {
                        Attribute resolved = PromqlLabels.getLabel(raw.output(), label);
                        // TimeSeriesWithout reads the names of the labels it leaves out, present at the source or not
                        return resolved != null ? resolved : new ReferenceAttribute(Source.EMPTY, label, DataType.KEYWORD);
                    }).toList();
                    groupKeys.add(new Alias(source, key.name(), new TimeSeriesWithout(source, fields)));
                }
            }
            for (UnresolvedPromqlAttribute label : keys.labels()) {
                Attribute column = PromqlLabels.getLabel(raw, label.name());
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
        private LogicalPlan resolveAggregate(Aggregate node, List<UnresolvedPromqlAttribute> needs) {
            Alias value = (Alias) node.aggregates().getFirst();
            Keys keys = keys(grain(node.groupings(), value), needs, false);
            List<UnresolvedPromqlAttribute> own = references(node);
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
        private LogicalPlan resolveRegroup(Project project, UnpackDims unpack, List<UnresolvedPromqlAttribute> needs) {
            Aggregate aggregate = (Aggregate) unpack.child();
            PackDims pack = (PackDims) aggregate.child();
            Alias value = (Alias) aggregate.aggregates().getFirst();
            Keys keys = keys(grain(pack.dims(), value), needs, false);
            List<UnresolvedPromqlAttribute> own = concat(references(pack), references(aggregate));
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
        private record Grain(@Nullable UnresolvedPromqlAttribute metadata, List<UnresolvedPromqlAttribute> labels, boolean perSeries) {}

        private Grain grain(List<? extends Expression> keyExpressions, Alias value) {
            UnresolvedPromqlAttribute metadata = null;
            var labels = new ArrayList<UnresolvedPromqlAttribute>();
            for (Expression key : keyExpressions) {
                if (key instanceof UnresolvedPromqlAttribute ref) {
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
            List<UnresolvedPromqlAttribute> metadata,
            List<UnresolvedPromqlAttribute> labels,
            List<UnresolvedPromqlAttribute> pushed,
            boolean declared
        ) {
            List<UnresolvedPromqlAttribute> all() {
                return concat(metadata, labels);
            }
        }

        /**
         * A need rides along an aggregate when the aggregate's grain determines it. The initial aggregate of a series-preserving
         * operator keeps one row per series, which determines every label and all metadata; it reads one metadata at most.
         */
        private Keys keys(Grain grain, List<UnresolvedPromqlAttribute> needs, boolean initial) {
            boolean perSeries = initial && grain.perSeries();
            var metadata = new ArrayList<UnresolvedPromqlAttribute>();
            if (grain.metadata() != null) {
                metadata.add(grain.metadata());
            }
            var carried = new ArrayList<UnresolvedPromqlAttribute>();
            for (UnresolvedPromqlAttribute need : needs) {
                if (need.isMetadata()) {
                    if (perSeries && metadata.stream().noneMatch(need::reads)) {
                        if (metadata.isEmpty()) {
                            metadata.add(need);
                        } else if (typed) {
                            // the source reads the finest metadata needed; a reader of a coarser one leaves labels out of it
                            int coarser = indexOf(metadata, read -> read.unset().containsAll(need.unset()));
                            if (coarser >= 0) {
                                metadata.set(coarser, need);
                            } else if (metadata.stream().noneMatch(read -> need.unset().containsAll(read.unset()))) {
                                // neither view derives from the other (a join key under ignoring(pod) and a group_left result
                                // leaving region out): the source reads what both leave out, and each reader derives its own
                                UnresolvedPromqlAttribute read = metadata.getFirst();
                                var common = new LinkedHashSet<>(read.unset());
                                common.retainAll(need.unset());
                                UnresolvedPromqlAttribute finest = read.againstSource(common);
                                unsetByName.putIfAbsent(finest.name(), finest.unset());
                                metadata.set(0, finest);
                            }
                        } else {
                            // Keyword _timeseries cannot derive a second view; require typed metadata.
                            throw unsupported(need);
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

        private VerificationException unsupported(UnresolvedPromqlAttribute ref) {
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
                    Attribute column = attribute instanceof UnresolvedPromqlAttribute ref ? byName.get(ref.name())
                        : attribute.resolved() ? null
                        : byId.getOrDefault(attribute.id(), byExactName.get(attribute.name()));
                    return column != null ? column : attribute;
                });
            }

            Attribute column(UnresolvedPromqlAttribute ref) {
                return byName.get(ref.name());
            }
        }

        /**
         * Binds the references of {@code node} and the {@code keys} it groups by to columns of {@code child}. What the child
         * lacks is a null column defined right above it, under the reference's id where the node has one.
         */
        private Binding bind(
            LogicalPlan node,
            LogicalPlan child,
            List<UnresolvedPromqlAttribute> own,
            List<UnresolvedPromqlAttribute> keys
        ) {
            return bind(node, child, own, keys, Map.of());
        }

        /** As above; a column the rule defines for a reference {@code declared} under another id takes that id. */
        private Binding bind(
            LogicalPlan node,
            LogicalPlan child,
            List<UnresolvedPromqlAttribute> own,
            List<UnresolvedPromqlAttribute> keys,
            Map<NameId, NameId> declared
        ) {
            var byName = new HashMap<String, Attribute>();
            var nulls = new ArrayList<Alias>();
            var ownByName = new HashMap<String, UnresolvedPromqlAttribute>();
            own.forEach(ref -> ownByName.putIfAbsent(ref.name(), ref));
            for (UnresolvedPromqlAttribute ref : concat(keys, own)) {
                if (byName.containsKey(ref.name())) {
                    continue;
                }
                Attribute column = column(child.output(), ref);
                if (column == null && ref.isMetadata() && typed == false) {
                    throw unsupported(ref);
                }
                if (column == null && ref.isMetadata()) {
                    // typed metadata the input carries a finer form of: leave the extra labels out with ts_unset
                    Attribute input = finerMetadata(child.output(), ref.unset());
                    if (input == null) {
                        throw unsupported(ref);
                    }
                    var labels = new LinkedHashSet<>(ref.unset());
                    labels.removeAll(unsetByName.get(input.name()));
                    UnresolvedPromqlAttribute read = ownByName.get(ref.name());
                    Alias metadata = new Alias(
                        ref.source(),
                        ref.name(),
                        unset(ref.source(), input, labels),
                        read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId()
                    );
                    nulls.add(metadata);
                    column = metadata.toAttribute();
                }
                if (column == null && typed && VectorBinaryOperator.EMPTY_SIGNATURE_KEY.equals(ref.name())) {
                    // the signature slot of an operand without metadata: the empty label set, which an opaque sibling's
                    // view equals once it has no label left outside the named key slots
                    UnresolvedPromqlAttribute read = ownByName.get(ref.name());
                    NameId id = read != null ? declared.getOrDefault(read.id(), read.id()) : new NameId();
                    Alias empty = new Alias(
                        ref.source(),
                        ref.name(),
                        new TimeSeriesFromKeyword(ref.source(), Literal.keyword(ref.source(), "{}")),
                        id
                    );
                    nulls.add(empty);
                    column = empty.toAttribute();
                }
                if (column == null && ref.isMetadata() == false && typed) {
                    // Opaque metadata holds the label: materialize it with ts_get (label_replace/join, on(...), by(...)).
                    Attribute metadata = metadataKeeping(child.output(), ref.name());
                    if (metadata != null) {
                        UnresolvedPromqlAttribute read = ownByName.get(ref.name());
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
                    UnresolvedPromqlAttribute read = ownByName.get(ref.name());
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
        private List<UnresolvedPromqlAttribute> references(LogicalPlan node) {
            var own = new ArrayList<UnresolvedPromqlAttribute>();
            node.forEachExpression(UnresolvedPromqlAttribute.class, ref -> addIfAbsent(own, ref));
            return own;
        }

        /** The column of {@code output} carrying {@code ref}: a label by its canonical name, metadata by its exact name. */
        private @Nullable Attribute column(List<Attribute> output, UnresolvedPromqlAttribute ref) {
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
                if (aggregate instanceof UnresolvedPromqlAttribute) {
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

        private static int indexOf(List<UnresolvedPromqlAttribute> refs, Predicate<UnresolvedPromqlAttribute> test) {
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

        private static void addIfAbsent(List<UnresolvedPromqlAttribute> refs, UnresolvedPromqlAttribute ref) {
            if (refs.stream().noneMatch(ref::reads)) {
                refs.add(ref);
            }
        }

        private static List<UnresolvedPromqlAttribute> concat(
            List<UnresolvedPromqlAttribute> first,
            List<UnresolvedPromqlAttribute> second
        ) {
            var result = new ArrayList<UnresolvedPromqlAttribute>(first);
            second.forEach(ref -> addIfAbsent(result, ref));
            return result;
        }
    }
}
