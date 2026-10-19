/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
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
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.LabelEval;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.Count.zeroCountAsNull;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * Shared state and assembly helpers for one PromQL translation. Requirements descend the tree; results carry
 * their label representation back to the consumer. This context binds labels, preserves aggregation boundaries,
 * and assembles the command output.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* Columns required by the consumer; metadata projections are materialized by initial aggregation. */
    TranslationSchema required,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** The command exposes every label of the result series: the full open label space. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, open(), null);
    }

    /** Carries a consumer's requirements without discarding metadata exclusions needed before source aggregation. */
    public TranslationContext withRequirement(TranslationSchema childRequired) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, childRequired, time);
    }

    /** Source labels for the typed representation; null requests the complete label set. */
    public @Nullable Set<String> requiredLabels() {
        return required.isOpen() ? null : required.labels();
    }

    /** Uses typed metadata only when it is enabled and all participating nodes can read it. */
    public boolean usesTimeSeriesMetadata() {
        return usesTimeSeriesMetadata(analyzer);
    }

    /** Keeps admission and translation on the same metadata representation for the participating nodes. */
    public static boolean usesTimeSeriesMetadata(AnalyzerContext analyzer) {
        return PromqlCommand.FUNCTIONAL_TIMESERIES_FEATURE_FLAG.isEnabled()
            && TimeSeriesMetadataAttribute.supportTimeSeriesMetadataType(analyzer.minimumVersion());
    }

    /** WITHOUT always drops the metric name, even when the exclusion list is empty. */
    public static List<String> withoutExclusions(List<String> labels) {
        if (labels.contains(LabelMatcher.NAME) == false) {
            var exclusions = new ArrayList<>(labels);
            exclusions.add(LabelMatcher.NAME);
            return exclusions;
        }
        return labels;
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * requirements travel down the PromQL tree and intermediate results travel back up.
     */
    public IntermediateResult translate(LogicalPlan node) {
        if (node instanceof PromqlPlan promql) {
            return promql.translate(this);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** Translates an independent branch with its own value and step identities. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, required, branchTime);
        return run.translateIntermediate(branch, valueId);
    }

    /** Combines independent branches and projects their labels to the declared command output. */
    public LogicalPlan translateFinal() {
        if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                return finishCommand(emitFinalProjection(op.translateJoin(this).plan()), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(cmd.promqlPlan(), branches);

        if (branches.size() == 1) {
            IntermediateResult intermediateResult = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            if (intermediateResult.kind() == Kind.EMPTY) {
                return new LocalRelation(cmd.source(), cmd.output(), EmptyLocalSupplier.EMPTY);
            }
            return finishCommand(emitFinalProjection(intermediateResult), intermediateResult.kind().constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        var intermediateResultPlan = doTranslateUnion(
            branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return finishCommand(emitFinalProjection(intermediateResultPlan), false);
    }

    /**
     * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
     * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public IntermediateResult eval(IntermediateResult t, Expression value) {
        if (t.kind().afterInitialAggregation == false) {
            return t.with(t.plan(), value);
        }
        Alias alias = new Alias(value.source(), valueColumnName(), value);
        return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute());
    }

    /**
     * Adapts a child's labels for its consumer. Named columns are reused; metadata is read through LabelEval or
     * rewritten with TS_UNSET. This is the integration point for label representations, so callers only declare
     * which labels they need. Published bindings are authoritative: incidental source columns cannot restore
     * labels removed by a child. Including no labels clears the grouping; excluding none removes only the metric name.
     */
    public IntermediateResult addInput(Source source, IntermediateResult input, List<Attribute> labels, boolean remove) {
        if (input.legacySchema() != null) {
            List<String> names = PromqlLabels.labelNames(labels);
            TranslationSchema schema = remove ? regroupWithout(input.schema(), withoutExclusions(names)) : TranslationSchema.finite(names);
            return input.with(input.plan(), schema, input.value());
        }
        if (remove) {
            if (input.kind() == Kind.BEFORE_INITIAL_AGGREGATE && input.metadata() != null) {
                // Source readers resolve aliases against the shard mapping. Keep the requested field spelling:
                // a qualified field and its short name may refer to different dimensions.
                var sourceExclusions = new LinkedHashSet<String>();
                for (Attribute label : labels) {
                    sourceExclusions.add(label instanceof FieldAttribute field ? field.fieldName().string() : label.name());
                }
                sourceExclusions.add(LabelMatcher.NAME);
                UnaryOperator<TimeSeriesMetadataAttribute> projectMetadata = metadata -> {
                    var excluded = new LinkedHashSet<>(metadata.excludedFields());
                    excluded.addAll(sourceExclusions);
                    return metadata.withExcludedFields(excluded);
                };
                LogicalPlan plan = input.plan().transformExpressionsDown(TimeSeriesMetadataAttribute.class, projectMetadata);
                Attribute metadata = input.metadata() instanceof TimeSeriesMetadataAttribute field
                    ? projectMetadata.apply(field)
                    : input.metadata();
                input = input.withMetadata(plan, metadata);
            }
            // Use logical names after loading: normalization can expose labels.__name__ without a mapping alias.
            var names = withoutExclusions(PromqlLabels.labelNames(labels));
            if (input.metadata() != null) {
                Attribute metadata = input.metadata();
                var unset = new TimeSeriesUnset(
                    source,
                    metadata,
                    names.stream().map(name -> (Expression) Literal.keyword(source, name)).toList()
                );
                // Replace the carrier so the result still has exactly one complete metadata value.
                var alias = new Alias(source, metadata.name(), unset);
                return input.withMetadata(new Eval(source, input.plan(), List.of(alias)), alias.toAttribute());
            }
            var bindings = new LinkedHashMap<>(input.labelBindings());
            names.forEach(bindings::remove);
            return input.withLabels(input.plan(), bindings);
        }
        var aliases = new ArrayList<Alias>();
        var bindings = new LinkedHashMap<String, Attribute>();
        Attribute metadata = input.metadata();
        for (Attribute label : labels) {
            String name = PromqlLabels.labelName(label);
            Attribute column = bindings.get(name);
            if (column == null) {
                Expression field = metadata != null ? metadata : input.labelColumn(name);
                if (field == null) {
                    field = new Literal(label.source(), null, label.dataType());
                }
                Expression value = LabelEval.create(label.source(), field, name, label.dataType(), configuration());
                if (value instanceof Attribute attribute) {
                    column = attribute;
                } else {
                    var alias = new Alias(label.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("label"), value);
                    aliases.add(alias);
                    column = alias.toAttribute();
                }
                bindings.put(name, column);
            }
            if (column.dataType() != label.dataType()) {
                throw new IllegalArgumentException("Conflicting types for label [" + name + "]");
            }
        }
        LogicalPlan plan = aliases.isEmpty() ? input.plan() : new Eval(source, input.plan(), aliases);
        return input.withLabels(plan, bindings);
    }

    /** Keeps the child columns that survive a WITHOUT aggregation and its consumer's requirements. */
    public TranslationSchema regroupWithout(TranslationSchema child, List<String> keys) {
        TranslationSchema schema = intersect(child, keys);
        assert child.isOpen() == false || schema.isOpen()
            : "invariant: required [" + required() + "] must declare a metadata column excluding " + keys + ", got " + child;
        return schema.isOpen() ? project(schema, required().labels()) : schema;
    }

    /**
     * The initial aggregate: a raw table collapsed to one row per step and schema column by the innermost
     * {@link TimeSeriesAggregate}, {@code function} applied in it. Passing the table's own value collapses it as is.
     */
    public IntermediateResult collapse(IntermediateResult input, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        Alias value = new Alias(function.source(), valueColumnName(), function);
        return table(emitCollapse(input, value, null), input, value);
    }

    /** A selector or range function returns one sample per physical series, even when normalized labels coincide. */
    private IntermediateResult collapseSeries(IntermediateResult input) {
        Attribute tsid = find(input.plan().output(), MetadataAttribute.TSID_FIELD);
        LogicalPlan plan = input.plan();
        if (tsid == null) {
            tsid = new MetadataAttribute(plan.source(), MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false);
            Attribute series = tsid;
            plan = plan.transformUp(EsRelation.class, relation -> {
                var attributes = new ArrayList<>(relation.output());
                attributes.add(series);
                return relation.withAttributes(attributes);
            });
        }
        input = input.with(plan, input.value());
        Alias value = new Alias(input.value().source(), valueColumnName(), input.value());
        return table(emitCollapse(input, value, tsid), input, value);
    }

    /** Applies a consumer's declaration before the initial aggregation materializes its columns. */
    public IntermediateResult collapse(IntermediateResult input, TranslationSchema schema, Expression function) {
        return collapse(input.with(input.plan(), schema, input.value()), function);
    }

    /**
     * Regroups a collapsed table. Typed named labels always preserve complete multivalued values; legacy plans
     * retain their requested grouping semantics. Metadata already forms a single scalar grouping key.
     */
    public IntermediateResult regroup(IntermediateResult input, TranslationSchema schema, boolean packDimensions, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a regroup takes a collapsed table";
        if (input.legacySchema() != null) {
            input = input.with(input.plan(), schema, input.value());
            packDimensions = schema.isOpen() || packDimensions;
        } else {
            packDimensions = input.metadata() == null;
        }
        Alias value = new Alias(function.source(), valueColumnName(), function);
        return table(emitRegroup(input, value, packDimensions), input, value);
    }

    private static IntermediateResult table(LogicalPlan plan, IntermediateResult input, Alias value) {
        return input.rebuild(plan, value.toAttribute(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The innermost aggregate preserves per-series evaluation before grouping by the requested result labels.
     */
    private LogicalPlan emitCollapse(IntermediateResult input, Alias value, @Nullable Attribute series) {
        Source source = cmd.promqlPlan().source();
        LogicalPlan plan = input.plan();
        boolean groupsBySeries = input.schema().isOpen() || input.schema().labels().isEmpty() == false;
        Expression agg = value.child();
        // Over-time functions become phase-1 references. The second phase needs Values unless an across-series
        // aggregate survives, possibly inside a scalar wrapper that the regular aggregate rules extract into Eval.
        // Ungrouped constants retain their existing literal path through TranslateTimeSeriesAggregate.
        if (groupsBySeries && hasAcrossSeriesAggregate(agg) == false) {
            value = value.replaceChild(new Values(agg.source(), agg));
        }

        // The first phase groups raw samples by _tsid. These keys define the second phase's label set.
        var groupKeys = new ArrayList<NamedExpression>();
        if (input.legacySchema() != null) {
            // Older nodes materialize metadata projections at source aggregation rather than through scalar functions.
            for (Set<String> excluded : coarse(input.schema().skips())) {
                List<Expression> fields = excluded.stream().<Expression>map(label -> {
                    Attribute resolved = find(plan.output(), label);
                    return resolved != null ? resolved : stub(label);
                }).toList();
                Alias metadata = new Alias(source, mapOpen(excluded), new TimeSeriesWithout(source, fields));
                groupKeys.add(metadata);
            }
            for (String label : input.schema().labels()) {
                Attribute column = find(plan.output(), label);
                if (column != null) {
                    groupKeys.add(column);
                }
            }
        } else {
            groupKeys.addAll(input.keys());
        }
        if (series != null) {
            groupKeys.add(series);
        }

        return new TimeSeriesAggregate(
            source,
            plan,
            groupings(stepBucketAlias, groupKeys),
            aggregates(value, input.step(), groupKeys.stream().map(NamedExpression::toAttribute).toList()),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
    }

    /**
     * Regroups an already-aggregated table using its current label bindings. Named labels use {@link PackDims}
     * to prevent multi-valued dimensions from splitting rows and double-counting. Metadata is already one scalar key.
     */
    private LogicalPlan emitRegroup(IntermediateResult input, Alias value, boolean packDimensions) {
        Source source = cmd.source();
        Attribute step = input.step();
        LogicalPlan plan = input.plan();
        if (hasAcrossSeriesAggregate(value.child()) == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        List<Attribute> keys;
        if (input.legacySchema() != null) {
            var nulls = new ArrayList<Alias>();
            keys = new ArrayList<>();
            for (Set<String> excluded : coarse(input.schema().skips())) {
                Attribute column = metadata(input, excluded);
                assert column != null : "metadata projection " + excluded + " must be carried by the child";
                keys.add(column);
            }
            for (String label : input.schema().labels()) {
                Attribute column = find(plan.output(), label);
                if (column == null) {
                    Alias missing = emitNullExpression(stub(label));
                    nulls.add(missing);
                    column = missing.toAttribute();
                }
                keys.add(column);
            }
            if (nulls.isEmpty() == false) {
                plan = new Eval(source, plan, nulls);
            }
        } else {
            keys = input.keys();
        }

        if (packDimensions == false || keys.isEmpty()) {
            return new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
        }
        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keys, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keys.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        return new Project(source, unpackDims, projections);
    }

    /** Stops at aggregate boundaries: functions inside an over-time aggregate do not survive as second-phase aggregates. */
    private static boolean hasAcrossSeriesAggregate(Expression expression) {
        if (expression instanceof AggregateFunction) {
            return expression instanceof TimeSeriesAggregateFunction == false;
        }
        return expression.children().stream().anyMatch(TranslationContext::hasAcrossSeriesAggregate);
    }

    /** Defines public result expressions together, then projects their attributes and the surviving labels. */
    private LogicalPlan emitFinalProjection(IntermediateResult result) {
        if (result.legacySchema() != null) {
            Attribute declared = find(cmd.output(), mapOpen(Set.of()));
            return emitFinalProjection(emitTimeSeriesAlias(result, declared != null ? declared.id() : new NameId()));
        }
        var aliases = new ArrayList<Alias>();
        var projected = new ArrayList<NamedExpression>();
        for (Attribute declared : cmd.output()) {
            Attribute actual;
            if (declared.id().equals(cmd.valueId())) {
                actual = result.valueColumn();
            } else if (declared.id().equals(cmd.stepId())) {
                actual = result.step();
            } else if (result.metadata() != null) {
                var rendered = new Alias(
                    declared.source(),
                    declared.name(),
                    new ToString(cmd.source(), result.metadata(), configuration()),
                    declared.id()
                );
                aliases.add(rendered);
                actual = rendered.toAttribute();
            } else {
                actual = result.labelColumn(PromqlLabels.labelName(declared));
            }
            if (actual == null) {
                throw new IllegalStateException("Missing output column [" + declared.name() + "]");
            }
            if (actual.semanticEquals(declared) && actual.name().equals(declared.name())) {
                projected.add(actual);
            } else {
                Alias alias = new Alias(declared.source(), declared.name(), actual, declared.id());
                projected.add(alias);
            }
        }
        LogicalPlan plan = aliases.isEmpty() ? result.plan() : new Eval(cmd.source(), result.plan(), aliases);
        return new Project(cmd.source(), plan, projected);
    }

    /** Keeps only steps within the query range; steps are anchored at {@code start} and offset-independent. */
    private LogicalPlan emitByStepFilter(LogicalPlan plan) {
        var source = cmd.source();
        var step = cmd.stepAttribute();
        var start = cmd.start();
        var end = cmd.end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /**
     * The source-time pushdown predicate. Expressed over the <b>raw</b> source timestamp (not the offset-shifted
     * evaluation timestamp) so it can push down to the index; the branch offset is instead folded into the bounds.
     * Expressing it over the shifted timestamp while also adjusting the bounds would apply the offset twice.
     */
    private Expression emitBySrcTimeFilter(LogicalPlan branch) {
        if (cmd.start().value() == null || cmd.end().value() == null) {
            return null;
        }
        var source = cmd.source();
        var offset = cmd.collectFirstOffsetForBranch(branch);
        var timestamp = cmd.timestamp();
        var window = cmd.sourceFilterWindow();
        var lo = new Sub(source, cmd.start(), Literal.timeDuration(source, window.plus(offset)), configuration());
        var hi = new Sub(source, cmd.end(), Literal.timeDuration(source, offset), configuration());
        return new And(source, new GreaterThanOrEqual(source, timestamp, lo), new LessThanOrEqual(source, timestamp, hi));
    }

    /** Filters raw samples before defining the evaluation timestamp; source setup happens once at the selector. */
    public LogicalPlan prepareSource(LogicalPlan plan, Selector selector, Expression matcher) {
        Expression filter = combineAndNullable(Arrays.asList(matcher, emitBySrcTimeFilter(selector)));
        if (filter != null) {
            plan = new Filter(cmd.source(), plan, filter);
        }
        Alias evaluationTime = evaluationTimestamp(selector);
        if (evaluationTime != null) {
            plan = new Eval(cmd.source(), plan, List.of(evaluationTime));
        }
        return plan;
    }

    /** Pushes a branch's deferred predicate to the source before aggregation. */
    private LogicalPlan pushDownSrcTimestampFilter(LogicalPlan plan, Expression filterCondition) {
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof Filter f && f.child() instanceof EsRelation) {
                return new Filter(f.source(), f.child(), new And(f.source(), f.condition(), filterCondition));
            } else if (p instanceof EsRelation) {
                return new Filter(cmd.source(), p, filterCondition);
            }
            return p;
        });
    }

    /** The value column definition: the translateIntermediate's value expression, cast to double unless it provably is one. */
    private Alias emitValueDoubleCastExpression(Expression valueExpr, NameId valueId) {
        if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
            valueExpr = new ToDouble(cmd.source(), valueExpr);
        }
        return new Alias(cmd.source(), valueColumnName(), valueExpr, valueId);
    }

    /**
     * The {@code step} bucket for a branch: the {@link TStep} grouping key shared across all aggregation groupings,
     * derived from the (possibly offset-shifted) evaluation timestamp - so an {@code offset} shifts which samples
     * fall into each fixed output bucket without moving the buckets. {@code stepId} names the synthetic column.
     */
    private Alias emitStepBucketExpression(NameId stepId, Expression time) {
        Expression size;
        Expression start;
        Expression end;
        if (cmd.isInstantQuery()) {
            size = Literal.timeDuration(cmd.source(), cmd.resolveInstantQueryWindow());
            start = new Sub(cmd.source(), cmd.start(), size, configuration());
            end = cmd.end();
        } else {
            size = cmd.resolveTimeBucketSize();
            start = cmd.start().value() != null ? cmd.start() : Literal.dateTime(cmd.source(), EPOCH_MIN);
            end = cmd.end().value() != null ? cmd.end() : Literal.dateTime(cmd.source(), EPOCH_MAX);
        }
        var tstep = new TStep(size.source(), size, start, end, time, configuration());
        return new Alias(tstep.source(), cmd.stepColumnName(), tstep, stepId);
    }

    private boolean canCreateStepBucket() {
        if (cmd.timestamp() == null || cmd.timestamp().resolved() == false) {
            return cmd.isRangeQuery() == false || cmd.buckets() == null || cmd.buckets().value() == null;
        }
        return true;
    }

    private static List<Expression> groupings(Expression step, List<? extends NamedExpression> keys) {
        var groupings = new ArrayList<Expression>(keys.size() + 1);
        groupings.add(step);
        groupings.addAll(keys);
        return groupings;
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, List<? extends NamedExpression> keys) {
        var aggregates = new ArrayList<NamedExpression>(keys.size() + 2);
        aggregates.add(value);
        aggregates.add(step);
        aggregates.addAll(keys);
        return aggregates;
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    /** Every command drops missing samples and bounds its evaluation steps after exposing the public columns. */
    private LogicalPlan finishCommand(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), plan, cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /** Exposes the surviving metadata projection under the command's public name. */
    private LogicalPlan emitTimeSeriesAlias(IntermediateResult table, NameId id) {
        Set<String> skip = table.schema().finestSkip();
        if (skip == null || skip.isEmpty()) {
            return table.plan();
        }
        Attribute column = metadata(table, skip);
        assert column != null : "metadata projection " + skip + " must be carried by the finished table";
        return new Eval(cmd.source(), table.plan(), List.of(new Alias(cmd.source(), MetadataAttribute.TIMESERIES, column, id)));
    }

    /** Combines independent union branches and keeps the leftmost value for each series and step. */
    private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
        // Already validated against max_branch_count_per_merge by PromqlCommand.verify.
        int maxBranches = analyzer.maxBranchCountPerMerge();
        assert intermediateResults.size() <= maxBranches
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be <= the max_branch_count_per_merge limit ["
                + maxBranches
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            var ir = intermediateResults.get(i);
            LogicalPlan branchPlan = emitNullsFilter(source, emitTimeSeriesAlias(ir, new NameId()), ir.valueColumn());
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branchTagExpression));
            // Each branch executes as an independent sub plan whose result pages cross an exchange, and the
            // consumer assumes their layout matches output() exactly. An Eval below (e.g. the value double-cast)
            // can name-shadow an existing column: the shadowed attribute leaves output() but its channel stays
            // in the page. An explicit projection pins the page layout to the branch output (see #158164).
            branchPlans.add(new Project(source, tagged, tagged.output()));
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
        // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        // Left-preferring dedup: group by every column except the value and the branch tag, keep the lowest branch.
        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /** Finishes a branch after its operands have contributed their deferred source predicates. */
    private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        IntermediateResult ir = doTranslateTryInline(translate(branch));

        var plan = ir.plan();
        var value = ir.value();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources - skip over InnerJoin.
        Expression timeFilter = usesTimeSeriesMetadata() || plan.anyMatch(p -> p instanceof InnerJoin) ? null : emitBySrcTimeFilter(branch);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }

        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                IntermediateResult collapsed = usesTimeSeriesMetadata()
                    ? collapseSeries(ir.with(plan, value))
                    : collapse(ir.with(plan, value), value);
                plan = collapsed.plan();
                value = collapsed.value();
                ir = collapsed;
            }
            if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                VectorMatch match = comparison.match();
                if ((match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) == false) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    // A vector-matched comparison already applied its filter inside the join translation.
                    ToDouble right = new ToDouble(comparison.right().source(), ((LiteralSelector) comparison.right()).literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), value, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }
        }

        // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (usesTimeSeriesMetadata() == false && ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? ir.kind() : Kind.AFTER_INITIAL_AGGREGATE;
        return ir.rebuild(plan, valueAlias.toAttribute(), null, kind);
    }

    /** Folds a value depending only on the evaluation step into a local relation. */
    private IntermediateResult doTranslateTryInline(IntermediateResult result) {
        Attribute stepAttr = cmd.stepAttribute();
        if (usesTimeSeriesMetadata()
            || result.kind().constant
            || cmd.start().value() == null
            || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new IntermediateResult(plan, result.schema(), value, step, result.pendingFilter(), Kind.CONSTANT);
    }

    /** Projects the command output, resolving public labels to their backing columns. */
    private LogicalPlan emitFinalProjection(LogicalPlan plan) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name.
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(mapFinite(attr), attr);
        }
        var projected = new ArrayList<>(cmd.output());
        var evals = new ArrayList<Alias>();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i);
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
    }

    /** Defines the offset-adjusted evaluation timestamp at the source. */
    private LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
        Alias evaluationTime = evaluationTimestamp(branch);
        return evaluationTime == null
            ? plan
            : plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(evaluationTime)));
    }

    /** Source readers and fused branches use the same offset-adjusted timestamp definition. */
    private @Nullable Alias evaluationTimestamp(LogicalPlan branch) {
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(branch);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            return new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
        }
        return null;
    }

    /** Flattens a top-level union while preserving left-to-right precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    // -- core --

    /**
     * A translated subtree and the state not yet represented by plan operators. Typed labels are authoritative
     * bindings, either named columns or one complete metadata value. Older representations retain the schema
     * until initial aggregation materializes its requested metadata projections.
     */
    public record IntermediateResult(
        LogicalPlan plan,
        /* A sample expression until initial aggregation materializes it as a column. */
        Expression value,
        Attribute step,
        /* Deferred source predicates can be fused into the operand's aggregate functions. */
        Expression pendingFilter,
        Kind kind,
        Map<String, Attribute> labelBindings,
        @Nullable Attribute metadata,
        @Nullable TranslationSchema legacySchema
    ) {
        public IntermediateResult {
            labelBindings = Collections.unmodifiableMap(new LinkedHashMap<>(labelBindings));
            labelBindings.forEach((name, attribute) -> {
                Objects.requireNonNull(name);
                Objects.requireNonNull(attribute);
            });
            if (legacySchema != null && (metadata != null || labelBindings.isEmpty() == false)) {
                throw new IllegalArgumentException("A result must have one label representation");
            }
            if (metadata != null) {
                if (labelBindings.isEmpty() == false) {
                    throw new IllegalArgumentException("Time-series labels must be either named columns or one complete metadata value");
                }
                if (metadata.dataType() != DataType.TIMESERIES_DATA_TYPE) {
                    throw new IllegalArgumentException("Expected typed time-series metadata");
                }
            }
        }

        /** Publishes complete bindings for the typed representation without consulting incidental plan columns. */
        public IntermediateResult(
            LogicalPlan plan,
            Expression value,
            Attribute step,
            Expression pendingFilter,
            Kind kind,
            Map<String, Attribute> labelBindings,
            @Nullable Attribute metadata
        ) {
            this(plan, value, step, pendingFilter, kind, labelBindings, metadata, null);
        }

        /** The lifecycle of a result. Constant steps and known-empty input are both source-independent. */
        public enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true),
            EMPTY(true, true);

            /** A local relation needs no source filtering or aggregation. */
            public final boolean constant;
            /** Value expressions above this boundary must materialize before being consumed. */
            public final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        /** Rebuilds the plan and sample while retaining published labels by attribute identity. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return rebuild(plan, value, pendingFilter, kind);
        }

        /** Publishes named labels, leaving any incidental metadata column outside the result's label bindings. */
        public IntermediateResult withLabels(LogicalPlan plan, Map<String, Attribute> labels) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, null);
        }

        /** Replaces the complete metadata carrier after a scalar label transformation. */
        public IntermediateResult withMetadata(LogicalPlan plan, Attribute metadata) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind, Map.of(), Objects.requireNonNull(metadata));
        }

        private IntermediateResult rebuild(LogicalPlan plan, Expression value, Expression pendingFilter, Kind kind) {
            if (legacySchema != null) {
                return new IntermediateResult(plan, legacySchema, value, step, pendingFilter, kind);
            }
            if (plan == this.plan) {
                return value == this.value && pendingFilter == this.pendingFilter && kind == this.kind
                    ? this
                    : new IntermediateResult(plan, value, step, pendingFilter, kind, labelBindings, metadata);
            }
            var available = new HashMap<NameId, Attribute>();
            plan.output().forEach(attribute -> available.put(attribute.id(), attribute));
            var labels = new LinkedHashMap<String, Attribute>();
            labelBindings.forEach((name, previous) -> {
                Attribute current = available.get(previous.id());
                if (current == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                labels.put(name, current);
            });
            Attribute currentMetadata = metadata == null ? null : available.get(metadata.id());
            if (metadata != null && currentMetadata == null) {
                throw new IllegalStateException("Missing time-series metadata column [" + metadata.name() + "]");
            }
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, currentMetadata);
        }

        /** Physical grouping keys in logical label declaration order. */
        private List<Attribute> keys() {
            return metadata == null ? List.copyOf(labelBindings.values()) : List.of(metadata);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }

        /** Resolves published typed labels or a deferred source-schema column. */
        public Attribute labelColumn(String name) {
            return legacySchema != null ? find(plan.output(), name) : labelBindings.get(name);
        }

        /** Resolves a published label or a source column in a deferred schema. */
        public Attribute label(String name) {
            return labelColumn(name);
        }

        /** Label declaration for operators that compose results before their columns are materialized. */
        public TranslationSchema schema() {
            return legacySchema != null ? legacySchema : metadata == null ? TranslationSchema.finite(labelBindings.keySet()) : open();
        }

        /** Retains source requirements until the initial aggregate can materialize them. */
        public IntermediateResult(
            LogicalPlan plan,
            TranslationSchema schema,
            Expression value,
            Attribute step,
            Expression pendingFilter,
            Kind kind
        ) {
            this(plan, value, step, pendingFilter, kind, Map.of(), null, schema);
        }

        /** Legacy constructor for a raw input. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step) {
            this(plan, schema, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** Legacy constructor for a raw input with a selector predicate. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, schema, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** Rebuilds a result with its consumer's label declaration. */
        public IntermediateResult with(LogicalPlan plan, TranslationSchema schema, Expression value) {
            if (legacySchema != null) {
                return new IntermediateResult(plan, schema, value, step, pendingFilter, kind);
            }
            schema.requireExclusive();
            if (schema.isOpen()) {
                return rebuild(plan, value, pendingFilter, kind);
            }
            var available = new HashMap<NameId, Attribute>();
            plan.output().forEach(attribute -> available.put(attribute.id(), attribute));
            var labels = new LinkedHashMap<String, Attribute>();
            for (String name : schema.labels()) {
                Attribute previous = labelBindings.get(name);
                Attribute current = previous == null ? find(plan.output(), name) : available.get(previous.id());
                if (current == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                labels.put(name, current);
            }
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, null);
        }

    }

    // -- helpers --

    private String valueColumnName() {
        return usesTimeSeriesMetadata() ? TemporaryNameGenerator.locallyUniqueTemporaryName("value") : cmd.valueColumnName();
    }

    /** Stable name for a metadata projection consumed before typed metadata is available. */
    public static String mapOpen(Set<String> excluded) {
        return MetadataAttribute.TIMESERIES + (excluded.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(excluded)));
    }

    /** Resolves a metadata projection materialized by source aggregation. */
    public Attribute metadata(IntermediateResult table, Set<String> skip) {
        String name = mapOpen(skip);
        return find(table.plan().output(), name);
    }

    /** A missing label's reference, subsequently defined as null by its consumer. */
    public static Attribute stub(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /** Legacy metadata exclusions ordered from the most detailed label set to the least detailed. */
    public static List<Set<String>> coarse(Set<Set<String>> skips) {
        return skips.stream().sorted(Comparator.comparingInt(Set::size)).toList();
    }

    /** A null-valued column under the attribute's own name and id, typed like the attribute (keyword when unresolved). */
    public static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /** Finds a canonical label, preferring its backing field to a same-named bare reference. */
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

    /** Canonical names used to resolve columns in source-derived schemas. */
    public static List<String> mapFinite(Collection<? extends Attribute> attributes) {
        var names = new LinkedHashSet<String>();
        attributes.forEach(attribute -> names.add(mapFinite(attribute)));
        return new ArrayList<>(names);
    }

    private static String mapFinite(Attribute attribute) {
        return PromqlLabels.labelName(attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name());
    }

    /**
     * A range function over a series without a sample in the window has no element in Prometheus. A series can still
     * reach the aggregate without one: its rows carry only other fields, or an operand's matchers reject them inside an
     * aggregate shared with another operand. There {@code count_over_time} reads 0 and {@code present_over_time} false;
     * both map to null so the element drops like any other without a value.
     * <p>
     * {@code absent_over_time} is the mirror: it has an element only for a series without a sample, so only true stays.
     * A series the operand never selects reads true too - its matchers reject every row, or only another operand's metric
     * has values - so true stays only for a series the operand selects.
     */
    public static Expression emptyRangeAsNull(Expression function, Expression time) {
        if (function instanceof CountOverTime) {
            return zeroCountAsNull(function);
        }
        if (function instanceof PresentOverTime present) {
            return new Case(present.source(), present, List.of(Literal.TRUE, Literal.NULL));
        }
        if (function instanceof AbsentOverTime absent) {
            Expression absentFromSelected = new And(absent.source(), selectedBy(absent, time), absent);
            return new Case(absent.source(), absentFromSelected, List.of(Literal.TRUE, Literal.NULL));
        }
        return function;
    }

    /**
     * Whether the operand of {@code absent} reads a row of the series in the window; the operand's matchers apply once its
     * filter attaches to its time-series functions. A metric selector selects a series only where the metric has a value:
     * rows are read for every metric the aggregate references
     * ({@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics}), so a row without a value of
     * this one can come from another operand's metric. For a metric field {@code absent} therefore never stays true.
     */
    private static Expression selectedBy(AbsentOverTime absent, Expression time) {
        if (absent.field() instanceof Attribute field && field.resolved() && field.isMetric()) {
            return new Not(absent.source(), absent);
        }
        return new PresentOverTime(absent.source(), time, Literal.TRUE, absent.window());
    }
}
