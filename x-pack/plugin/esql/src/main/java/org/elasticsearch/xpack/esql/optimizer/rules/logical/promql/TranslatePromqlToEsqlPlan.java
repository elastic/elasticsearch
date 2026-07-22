/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.InheritedAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.ResolvedAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.SynthesizedAttributes;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramQuantile;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ValueTransformationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.isTimeSeriesAttributeName;
import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.canonicalName;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.concreteDimensions;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.findByLabelName;

/**
 * Translates PromQL logical plan into ESQL plan. Runs before {@link TranslateTimeSeriesAggregate} to convert
 * PromQL-specific nodes into standard ESQL nodes (TimeSeriesAggregate, Aggregate, Eval, etc.). Examples:
 * <pre>
 * PromQL: sum by (cluster) (rate(http_requests[5m]))
 * Result: TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: time() - avg(sum by (cluster) (rate(http_requests[5m])))
 * Result: Eval[time() - avg_result]
 *           \_ Aggregate[avg(sum_result), groupBy=[step]]
 *                 \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 * </pre>
 * Mechanism: a {@link Translation} instance per command; recursive descent via {@code doTranslateNode()} where every AST
 * node produces a {@link IntermediateResult} its parent composes, and the top-level forms (single translateIntermediate,
 * {@code or} union) stitch finished tables.
 */
public final class TranslatePromqlToEsqlPlan extends AnalyzerRules.ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** The shape of an intermediate result along its relation origin and inner-aggregation axes. */
    private enum Kind {
        BEFORE_INITIAL_AGGREGATE(false, false),
        AFTER_INITIAL_AGGREGATE(true, false),
        CONST_BEFORE_INITIAL_AGGREGATE(false, true),
        CONST_AFTER_INITIAL_AGGREGATE(true, true);

        final boolean constant;
        final boolean afterInitialAggregation;

        Kind(boolean afterInitialAggregation, boolean constant) {
            this.afterInitialAggregation = afterInitialAggregation;
            this.constant = constant;
        }
    }

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its defined columns. Every AST
     * node translates to a Table and the stitching operations (joins, unions, regroups, the command coda) compose tables
     * by their declared columns instead of rediscovering them in the plan output. Mid-descent {@code value} is a (possibly
     * not yet materialized) expression parents compose into larger expressions; a finished translateIntermediate's {@code value} is a
     * defined column ({@link #valueColumn()}) and its {@code step} is filled in.
     */
    private record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* This node's numeric value: an expression mid-descent, a defined column once the translateIntermediate is finished. */
        Expression value,
        /* Label matcher predicate; flows up until pushed to the relation or folded into an doTranslateAgg filter. */
        Expression pendingFilter,
        /* The label shape this subtree exposes. */
        SynthesizedAttributes labels,
        /* The step bucket column; null mid-descent (the context carries it) and when no step bucket was created. */
        Attribute step,
        /* The compiler tracks what it built instead of inspecting the plan. */
        Kind kind
    ) {
        IntermediateResult(LogicalPlan plan, Expression value) {
            this(plan, value, null, SynthesizedAttributes.none(), null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Expression selectorFilter) {
            this(plan, value, selectorFilter, SynthesizedAttributes.none(), null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Expression selectorFilter, SynthesizedAttributes labels) {
            this(plan, value, selectorFilter, labels, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan/value, keeping its other properties. */
        IntermediateResult with(LogicalPlan plan, Expression value, SynthesizedAttributes labels) {
            return new IntermediateResult(plan, value, pendingFilter, labels, step, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        Attribute valueColumn() {
            return (Attribute) value;
        }
    }

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(PromqlCommand cmd, AnalyzerContext context) {
        Translation translation = new Translation(cmd, context, null, InheritedAttributes.unconstrained(), null);
        return translation.translateFinal();
    }

    /**
     * One translation pass: the command (or an operand fork of it), the analyzer context, and the state of the translateIntermediate
     * being compiled. Independent parts compile separately - like modules - each with its own instance: a narrowed
     * grouping scope is {@link #withScope}, and a union branch translateIntermediate is a fresh instance with its own
     * step bucket and evaluation time.
     */
    private record Translation(
        PromqlCommand cmd,
        AnalyzerContext analyzer,
        /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
        Alias stepBucketAlias,
        /* The labels the child subtree MUST produce. */
        InheritedAttributes scope,
        /* The current translateIntermediate evaluation time (default: @timestamp). */
        Expression time
    ) {
        Configuration configuration() {
            return analyzer.configuration();
        }

        Attribute stepAttr() {
            return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
        }

        Translation withScope(InheritedAttributes newScope) {
            return new Translation(cmd, analyzer, stepBucketAlias, newScope, time);
        }

        /**
         * Translates one union branch translateIntermediate with its own step bucket and evaluation time.
         * {@code demanded} is the label scope the module must produce - the interface the surrounding query compiled
         * against (e.g. an outer {@code by (cluster)} demands the operand materialize {@code cluster} as a column).
         */
        IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId, InheritedAttributes demanded) {
            Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
            Alias step = canCreateStepBucket() ? stepBucket(stepId, branchTime) : null;
            var run = new Translation(cmd, analyzer, step, demanded, branchTime);
            return run.translateIntermediate(branch, valueId);
        }

        LogicalPlan translateFinal() {
            // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch UnionAll that
            // cannot compose as a single-value sub-expression.
            // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
            List<LogicalPlan> branches = new ArrayList<>();
            flattenUnion(cmd.promqlPlan(), branches);

            if (branches.size() == 1) {
                IntermediateResult intermediateResult = translateIntermediate(
                    cmd.promqlPlan(),
                    cmd.stepId(),
                    cmd.valueId(),
                    InheritedAttributes.unconstrained()
                );
                return doTranslateFinal(intermediateResult.plan(), intermediateResult.kind.constant);
            }
            // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
            return doTranslateFinal(
                doTranslateUnion(
                    branches.stream()
                        .map(b -> translateIntermediate(b, new NameId(), new NameId(), InheritedAttributes.unconstrained()))
                        .toList()
                ),
                false
            );
        }

        // -- helpers --

        /* Shared by every `final` translation root */
        private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation) {
            plan = dropNullRows(cmd.source(), projectCommandOutput(plan), cmd.valueAttribute());
            return localRelation ? plan : filterStepBounds(plan);
        }

        /**
         * Union combinator over independently translated tabular results.
         * {@link UnionAll} aligns columns by name and null-fills missing labels, then
         * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
         */
        private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
            // Already validated against Fork.MAX_BRANCHES by PromqlCommand.verify; asserted here against regressions.
            assert Fork.exceedsMaxBranches(intermediateResults.size()) == false
                : "union branch count [" + intermediateResults.size() + "] exceeds Fork.MAX_BRANCHES [" + Fork.MAX_BRANCHES + "]";

            Source source = cmd.source();
            List<LogicalPlan> branchPlans = new ArrayList<>(intermediateResults.size());
            for (int i = 0; i < intermediateResults.size(); i++) {
                // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
                LogicalPlan branchPlan = dropNullRows(source, intermediateResults.get(i).plan(), intermediateResults.get(i).valueColumn());
                Alias branchTag = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
                branchPlans.add(new Eval(source, branchPlan, List.of(branchTag)));
            }

            // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
            // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
            List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
            UnionAll union = new UnionAll(source, branchPlans, unionOutput);

            // Left-preferring dedup: group by every column except the value and the branch tag, keep the lowest branch.
            List<Expression> groupings = new ArrayList<>();
            Attribute branchAttr = null;
            for (Attribute attr : unionOutput) {
                if (attr.name().equals(cmd.branchColumnName())) {
                    branchAttr = attr;
                } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                    groupings.add(attr);
                }
            }
            Order order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
            return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
        }

        /**
         * Translates independent query fragment into intermediate result (IR).
         * Think of IR as table
         */
        private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
            IntermediateResult result = doTranslateTryConstFold(doTranslateNode(branch));

            var plan = result.plan();
            var valueExpr = result.value();
            Expression timeFilter = sourceTimeFilter(branch);
            var filter = combineAndNullable(Arrays.asList(result.pendingFilter(), timeFilter));
            if (filter != null) {
                plan = pushFilterToRelation(plan, filter);
            }

            if (result.kind.constant == false) {
                // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
                // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
                if (result.kind.afterInitialAggregation == false) {
                    plan = emitInnermostAggregate(
                        plan,
                        SynthesizedAttributes.of(branch.output()),
                        List.of(),
                        scope.demandedLabels(),
                        valueExpr
                    );
                    valueExpr = valueColumn(plan);
                }
                if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    ToDouble right = new ToDouble(comparison.right().source(), ((LiteralSelector) comparison.right()).literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), valueExpr, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }

            // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
            Alias value = castValueToDouble(valueExpr, valueId);
            plan = new Eval(cmd.source(), plan, List.of(value));
            if (result.kind.constant == false) {
                plan = materializeEvaluationTimestamp(plan, branch);
            }

            Attribute stepAttr = stepBucketAlias == null ? null : stepBucketAlias.toAttribute();
            Kind kind = result.kind.constant ? Kind.CONST_AFTER_INITIAL_AGGREGATE : Kind.AFTER_INITIAL_AGGREGATE;
            return new IntermediateResult(plan, value.toAttribute(), null, result.labels(), stepAttr, kind);
        }

        /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
        private IntermediateResult doTranslateTryConstFold(IntermediateResult result) {
            Attribute stepAttr = cmd.stepAttribute();
            if (result.kind.constant
                || cmd.start().value() == null
                || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
                return result;
            }
            var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
            var step = plan.output().getFirst();
            var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
            return new IntermediateResult(plan, value, result.pendingFilter(), result.labels(), null, Kind.CONST_AFTER_INITIAL_AGGREGATE);
        }

        /**
         * Recursively translates a PromQL plan node. The source relation {@code cmd.child()} is the leaf at the bottom of
         * the produced subtree; the PromQL tree is walked top-down and the ESQL plan assembled bottom-up on the way back.
         */
        private IntermediateResult doTranslateNode(LogicalPlan node) {
            return switch (node) {
                case AcrossSeriesAggregate agg -> doTranslateXSeriesAgg(agg);
                case AcrossSeriesReduction reduction -> doTranslateXSeriesReduction(reduction);
                case HistogramQuantile histogramQuantile -> doTranslateHq(histogramQuantile);
                case ScalarConversionFunction scalar -> doTranslateScalarConvertion(scalar);
                case PromqlFunctionCall functionCall -> doTranslateFunc(functionCall);
                case ScalarFunction scalarFunction -> doTranslateScalarFunc(scalarFunction);
                case VectorBinaryOperator binaryOp -> doTranslateBinaryOp(binaryOp);
                case Selector selector -> doTranslateSelector(selector);
                default -> throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
            };
        }

        /**
         * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
         * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
         */
        private IntermediateResult doTranslateAddValueEval(IntermediateResult t, Expression value, SynthesizedAttributes labels) {
            if (t.kind.afterInitialAggregation == false) {
                return t.with(t.plan(), value, labels);
            }
            Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
            return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute(), labels);
        }

        /**
         * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}. PromQL aggregation shape is dynamic and
         * can't be expressed in ESQL without enumerating the full label set; to avoid that at plan time the translator
         * walks the doTranslateAgg chain twice, carrying the grouping algebra of {@link PromqlAttributesTranslationContext}
         * (see that class for the transition rules): the inherited scope pushes down to the leaf, the synthesized shape
         * folds each grouping back up. Only {@code AcrossSeriesAggregate} creates plan-level aggregation nodes;
         * within-series aggregates and function calls lower to expressions folded into the doTranslateAgg.
         */
        private IntermediateResult doTranslateXSeriesAgg(AcrossSeriesAggregate agg) {
            // Descend: narrow the inherited scope and hand it to the child.
            var narrowed = inheritedGrouping(agg, scope);
            IntermediateResult child = withScope(narrowed).doTranslateNode(agg.child());
            if (child.kind.constant) {
                return child;
            }
            // Ascend: fold this doTranslateAgg's grouping over the shape the child synthesized; build the doTranslateAgg function
            // (sum, max, etc.) from the PromQL registry.
            var shape = synthesizedGrouping(agg, child);
            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, stepAttr(), configuration());
            return doTranslateAgg(child, child.plan(), shape, narrowed.pathExclusions(), agg.buildEsqlFunction(child.value(), promqlCtx));
        }

        /**
         * Translates an {@link AcrossSeriesReduction} ({@code topk}): collapse the child to one row per series, then rank
         * and keep the top {@code k}. A {@code by} clause only partitions the ranking; it does not change output labels.
         */
        private IntermediateResult doTranslateXSeriesReduction(AcrossSeriesReduction reduction) {
            if (reduction.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT) {
                throw new VerificationException("PromQL function [{}] does not yet support [without]", reduction.functionName());
            }

            IntermediateResult child = withScope(InheritedAttributes.unconstrained()).doTranslateNode(reduction.child());
            if (child.kind.constant) {
                return child;
            }

            var identity = SynthesizedAttributes.foldExcluding(List.of(), child.labels());
            List<Attribute> partitionLabels = reduction.grouping() == AcrossSeriesAggregate.Grouping.BY ? reduction.groupings() : List.of();
            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, stepAttr(), configuration());
            Expression agg = reduction.buildEsqlFunction(child.value(), promqlCtx);
            LogicalPlan result = child.kind.afterInitialAggregation
                ? emitOuterAggregate(child.plan(), identity, agg, partitionLabels)
                : emitInnermostAggregate(child.plan(), identity, List.of(), partitionLabels, agg);
            result = wrapWithTopNBy(reduction, result);
            return new IntermediateResult(
                result,
                valueColumn(result),
                child.pendingFilter(),
                identity,
                child.step(),
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }

        /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step. */
        private LogicalPlan wrapWithTopNBy(AcrossSeriesReduction reduction, LogicalPlan resultPlan) {
            var groupings = new ArrayList<Expression>();
            groupings.add(stepAttr());
            if (reduction.grouping() == AcrossSeriesAggregate.Grouping.BY) {
                for (Attribute label : reduction.groupings()) {
                    Attribute resolved = findByLabelName(resultPlan.output(), canonicalName(label));
                    groupings.add(resolved != null ? resolved : label);
                }
            }
            var order = List.of(
                new Order(reduction.source(), valueColumn(resultPlan), Order.OrderDirection.DESC, Order.NullsPosition.LAST)
            );
            Expression k = new ToInteger(reduction.source(), reduction.parameters().getFirst());
            return new TopNBy(reduction.source(), resultPlan, order, k, groupings);
        }

        /** The doTranslateAgg combinator: regroups a grouped table, or emits the innermost `_timeseries` doTranslateAgg over a raw one. */
        private IntermediateResult doTranslateAgg(
            IntermediateResult child,
            LogicalPlan plan,
            SynthesizedAttributes labels,
            List<Attribute> excl,
            Expression agg
        ) {
            LogicalPlan result = child.kind.afterInitialAggregation
                ? emitOuterAggregate(plan, labels, agg)
                : emitInnermostAggregate(plan, labels, excl, List.of(), agg);
            return new IntermediateResult(
                result,
                valueColumn(result),
                child.pendingFilter(),
                labels,
                child.step(),
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }

        private IntermediateResult doTranslateHq(HistogramQuantile hq) {
            IntermediateResult child = withScope(emitHqChildAttrs(hq)).doTranslateNode(hq.child());
            if (child.kind.constant) {
                return child;
            }

            // Classic (counter backed) histograms need the special treatment below; native histograms - distinguishable
            // only at this point in planning - are regular value transformations.
            if (child.value().resolved() && child.value().dataType().isHistogram()) {
                var definition = PromqlHistogramQuantile.PROMQL_DEFINITION;
                return doTranslateFunc(new ValueTransformationFunction(hq.source(), hq.child(), definition, hq.parameters()));
            }

            LogicalPlan childPlan = child.plan();
            Attribute upperBound = findByLabelName(child.labels().declared(), HistogramQuantile.LE_LABEL);
            if (upperBound == null && child.kind.afterInitialAggregation) {
                upperBound = findByLabelName(childPlan.output(), HistogramQuantile.LE_LABEL);
            }
            if (upperBound == null) {
                // Mirrors Prometheus, which warns and drops series whose `le` bucket label is missing.
                HeaderWarning.addWarning("histogram_quantile: input vector has no le label; no buckets to evaluate");
                SynthesizedAttributes exportLabels = preserveTimeseries(child.labels(), hq.child().output());
                LogicalPlan filteredChild = new Filter(hq.source(), childPlan, Literal.FALSE);
                Expression emptyResult = new Values(hq.source(), new Literal(hq.source(), null, DataType.DOUBLE));
                return doTranslateAgg(child, filteredChild, exportLabels, List.of(), emptyResult);
            }

            if (child.kind.afterInitialAggregation == false) {
                childPlan = emitInnermostAggregate(childPlan, child.labels(), List.of(upperBound), List.of(), child.value());
                child = new IntermediateResult(
                    childPlan,
                    valueColumn(childPlan),
                    child.pendingFilter(),
                    synthesizedLabels(childPlan),
                    child.step(),
                    Kind.AFTER_INITIAL_AGGREGATE
                );
                upperBound = findByLabelName(childPlan.output(), HistogramQuantile.LE_LABEL);
                assert upperBound != null : "histogram_quantile child materialization must expose le";
            }

            // histogram_quantile groups by every label except the `le` bucket label, so `le` is the single excluded
            // dimension - the synthesized shape drops it and the innermost `_timeseries` excludes it. Bucket counts are
            // consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
            SynthesizedAttributes exportLabels = SynthesizedAttributes.foldExcluding(List.of(upperBound), child.labels());
            Expression count = new ToDouble(hq.source(), child.value());
            Expression quantile = new PromqlHistogramQuantile(hq.source(), count, upperBound, hq.quantile());
            return doTranslateAgg(child, childPlan, exportLabels, List.of(), quantile);
        }

        /**
         * The child of histogram_quantile must expose the labels the surrounding query still needs (the inherited scope)
         * together with {@code le}: a finite scope (e.g. an outer {@code BY(job)}) preserves those labels as concrete
         * keys; for the full universe the enumerated dimensions stand in for it. With no concrete dimensions and no
         * {@code le}, fall back to the raw child output so its {@code _timeseries} identity survives.
         */
        private InheritedAttributes emitHqChildAttrs(HistogramQuantile hq) {
            List<Attribute> identity = concreteDimensions(hq.child().output());
            Attribute upperBound = findByLabelName(cmd.child().output(), HistogramQuantile.LE_LABEL);
            if (upperBound != null) {
                return scope.including(identity).including(List.of(upperBound));
            }
            return identity.isEmpty() ? scope.limitedTo(hq.child().output()) : scope.including(identity);
        }

        /** scalar(): collapse to one value per step, e.g. scalar(sum by (cluster) (metric)). */
        private IntermediateResult doTranslateScalarConvertion(ScalarConversionFunction scalarFunc) {
            IntermediateResult child = doTranslateNode(scalarFunc.child());
            if (child.value().foldable()) {
                return new IntermediateResult(child.plan(), new ToDouble(scalarFunc.source(), child.value()), child.pendingFilter());
            }
            Expression scalarExpr = new Scalar(scalarFunc.source(), child.value());
            return doTranslateAgg(child, child.plan(), SynthesizedAttributes.none(), List.of(), scalarExpr);
        }

        /** Translates a generic PromQL function call (rate, ceil, abs, etc.) into an expression over the child's value. */
        private IntermediateResult doTranslateFunc(PromqlFunctionCall functionCall) {
            IntermediateResult child = doTranslateNode(functionCall.child());
            if (child.kind.constant) {
                return child;
            }
            Expression window = AggregateFunction.NO_WINDOW;
            if (functionCall.child() instanceof RangeSelector rangeSelector) {
                window = isImplicitRangePlaceholder(rangeSelector.range()) ? cmd.resolveImplicitRangeWindow() : rangeSelector.range();
            }
            var promqlCtx = new PromqlContext(time, window, stepAttr(), configuration());
            return doTranslateAddValueEval(child, functionCall.buildEsqlFunction(child.value(), promqlCtx), child.labels());
        }

        /** Translates a scalar function (time(), etc.): an expression over the unchanged source. */
        private IntermediateResult doTranslateScalarFunc(ScalarFunction scalarFunction) {
            var function = scalarFunction.buildEsqlFunction(new PromqlContext(cmd.timestamp(), null, cmd.stepAttribute(), configuration()));
            return new IntermediateResult(cmd.child(), function);
        }

        /** Translates binary operators by composing the operator as an expression over a shared frame. */
        private IntermediateResult doTranslateBinaryOp(VectorBinaryOperator binaryOp) {
            return doTranslateMergeableBinaryOp(binaryOp);
        }

        private IntermediateResult doTranslateMergeableBinaryOp(VectorBinaryOperator binaryOp) {
            IntermediateResult left = doTranslateNode(binaryOp.left());
            Expression leftExpr = new ToDouble(left.value().source(), left.value());
            if (binaryOp instanceof VectorBinaryComparison comp && comp.filterMode()) {
                return left.with(left.plan(), leftExpr, left.labels());
            }

            IntermediateResult right = doTranslateNode(binaryOp.right());
            Expression rightExpr = new ToDouble(right.value().source(), right.value());
            Expression binaryExpr = binaryOp.binaryOp().asFunction().create(binaryOp.source(), leftExpr, rightExpr, configuration());

            LogicalPlan plan;
            Expression filter;
            if (left.kind.afterInitialAggregation && right.kind.afterInitialAggregation) {
                plan = foldBinaryOperatorAggregate(left, right);
                filter = null;
            } else {
                plan = left.kind.afterInitialAggregation ? left.plan() : right.plan();
                filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
            }
            SynthesizedAttributes shape = left.labels().hasDeclared() ? left.labels() : right.labels();
            Kind kind = left.kind.afterInitialAggregation || right.kind.afterInitialAggregation
                ? Kind.AFTER_INITIAL_AGGREGATE
                : Kind.BEFORE_INITIAL_AGGREGATE;
            IntermediateResult result = new IntermediateResult(plan, null, filter, shape, null, kind);
            return doTranslateAddValueEval(result, binaryExpr, shape);
        }

        /** Fold left and right aggregates into a single plan. */
        private LogicalPlan foldBinaryOperatorAggregate(IntermediateResult left, IntermediateResult right) {
            var names = new TemporaryNameGenerator.Monotonic();
            var rightAgg = right.plan().collect(Aggregate.class).getFirst();

            var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
                Set<String> leftGroupingNames = new HashSet<>();
                for (Expression grouping : leftAgg.groupings()) {
                    if (grouping instanceof NamedExpression ne) {
                        leftGroupingNames.add(ne.name());
                    }
                }
                Set<String> rightGroupingNames = new HashSet<>();
                for (Expression grouping : rightAgg.groupings()) {
                    if (grouping instanceof NamedExpression ne) {
                        rightGroupingNames.add(ne.name());
                    }
                }
                boolean groupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                    && leftGroupingNames.equals(rightGroupingNames);

                if (groupingsCompatible == false) {
                    throw new VerificationException(
                        "Binary expressions between vectors with different grouping keys are not supported yet. "
                            + "Left groupings: {}, right groupings: {}",
                        leftGroupingNames,
                        rightGroupingNames
                    );
                }

                var uniqueAggregates = new LinkedHashSet<Expression>();
                uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
                uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.pendingFilter()));

                var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                    Expression inner = e;
                    if (e instanceof Alias a) {
                        inner = a.child();
                    }
                    return new Alias(e.source(), names.next(e.name()), inner, e.id());
                }).toList();

                return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
            });

            var rightEvals = right.plan().collect(Eval.class);
            for (Eval eval : rightEvals.reversed()) {
                result = new Eval(eval.source(), result, eval.fields());
            }
            return result;
        }

        /** Translates a selector (instant, range, or literal); label matchers lower to a pending filter predicate. */
        private IntermediateResult doTranslateSelector(Selector selector) {
            LogicalPlan input = cmd.child();
            LogicalPlan foldedPlan = PromqlLogicalPlanBuilder.tryFoldRelation(cmd, input);
            Expression matcher = lowerMatchers(selector.source(), selector.labels(), selector.labelMatchers(), configuration());

            if (selector instanceof LiteralSelector literalSelector) {
                return foldedPlan != null
                    ? new IntermediateResult(
                        foldedPlan,
                        literalSelector.literal(),
                        matcher,
                        SynthesizedAttributes.none(),
                        null,
                        Kind.CONST_AFTER_INITIAL_AGGREGATE
                    )
                    : new IntermediateResult(input, literalSelector.literal(), matcher, SynthesizedAttributes.none());
            }
            if (foldedPlan != null) {
                var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
                return new IntermediateResult(
                    empty,
                    Literal.NULL,
                    null,
                    SynthesizedAttributes.none(),
                    null,
                    Kind.CONST_AFTER_INITIAL_AGGREGATE
                );
            }

            // An instant selector maps to LastOverTime to get the latest sample per time series.
            Expression expr = selector instanceof InstantSelector
                ? new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, time)
                : selector.series();
            // Non-literal selectors reflect the inherited scope back up; EsRelation must have the required dimensions.
            return new IntermediateResult(input, expr, matcher, scope.reflect());
        }

        /**
         * The innermost doTranslateAgg: its child is the leaf selector, so it owns the physical {@code _timeseries} grouping
         * and translates against its own scope, excluding every dimension dropped on the way down (the path exclusions).
         */
        private LogicalPlan emitInnermostAggregate(
            LogicalPlan plan,
            SynthesizedAttributes labels,
            List<Attribute> excl,
            List<Attribute> extraKeys,
            Expression agg
        ) {
            Source source = cmd.promqlPlan().source();
            var translation = labels.translateLeaf(excl);

            // Empty `without ()` retains the full label set T: translateLeaf surfaces a `_timeseries` grouping key for it
            // (with an empty exclusion set), so the `_timeseries` grouping check already covers it.
            boolean needsTimeSeriesGrouping = translation.groupings().stream().anyMatch(a -> isTimeSeriesAttributeName(a.name()))
                || translation.excludedDimensions().isEmpty() == false;
            // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
            // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
            // AggregateFunction inside the Aggregate node:
            // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
            // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
            // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
            // Guarded by needsTimeSeriesGrouping because without dimension grouping (e.g. constants like vector(5))
            // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
            boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
            if (needsTimeSeriesGrouping && wrapWithValues) {
                agg = new Values(agg.source(), agg);
            }

            var groupKeys = new ArrayList<NamedExpression>();
            var outKeys = new ArrayList<NamedExpression>();
            if (needsTimeSeriesGrouping) {
                // Resolve each excluded label to the concrete dimension column the child exposes, so the exclusion names
                // match the backing dimension fields the `_timeseries` block loader enumerates: PromQL `pod` must become
                // the stored dimension `labels.pod` for Prometheus passthrough data, otherwise the bare key never matches
                // and the label leaks into the output. A label absent from the child stays unresolved - simply a no-op.
                List<Expression> excluded = translation.excludedDimensions().stream().<Expression>map(label -> {
                    Attribute resolved = findByLabelName(plan.output(), canonicalName(label));
                    return resolved != null ? resolved : label;
                }).toList();
                var tsWithout = new TimeSeriesWithout(source, excluded).createNamedExpression();
                groupKeys.add(tsWithout);
                outKeys.add(tsWithout.toAttribute());
            }
            // Non `_timeseries` keys pass through as both groupings and outputs.
            for (Attribute key : translation.groupings()) {
                if (isTimeSeriesAttributeName(key.name()) == false) {
                    groupKeys.add(key);
                    outKeys.add(key);
                }
            }
            // Materialize the demanded labels as additional keys: a dimension is functionally dependent on the series id,
            // so grouping by it alongside `_timeseries` keeps per-series granularity while exposing the column for the
            // surrounding query (an operand module compiled under an outer by (...) must produce those columns).
            for (Attribute demanded : extraKeys) {
                Attribute resolved = findByLabelName(plan.output(), canonicalName(demanded));
                if (resolved != null && outKeys.stream().noneMatch(k -> k.name().equals(resolved.name()))) {
                    groupKeys.add(resolved);
                    outKeys.add(resolved);
                }
            }

            var value = new Alias(agg.source(), cmd.valueColumnName(), agg);
            var origin = TimeSeriesAggregate.Origin.PROMQL_COMMAND;
            var groupings = groupings(stepBucketAlias, groupKeys);
            return new TimeSeriesAggregate(source, plan, groupings, aggregates(value, stepAttr(), outKeys), null, time, origin);
        }

        /** Outer aggregation over an already-aggregated child: a packed WITHOUT regroup or a plain BY/NONE regroup. */
        private LogicalPlan emitOuterAggregate(LogicalPlan plan, SynthesizedAttributes labels, Expression aggExpr) {
            return emitOuterAggregate(plan, labels, aggExpr, List.of());
        }

        /** Outer aggregation over an already-aggregated child: a packed WITHOUT regroup or a plain BY/NONE regroup. */
        private LogicalPlan emitOuterAggregate(
            LogicalPlan plan,
            SynthesizedAttributes labels,
            Expression aggExpr,
            List<Attribute> extraKeys
        ) {
            NamedExpression value = new Alias(aggExpr.source(), cmd.valueColumnName(), aggExpr);
            var translation = labels.translate(plan.output());
            return labels.hasExclusions()
                ? emitPackedRegroup(plan, translation, value)
                : emitPlainRegroup(plan, translation, value, extraKeys);
        }

        /**
         * A WITHOUT regroup must pack its carried dimensions before aggregating - a multi-valued dimension would
         * otherwise split the row and double-count - then unpack after. Emits {@code Project <- Eval[unpack] <- Aggregate
         * <- Eval[pack] <- (Eval[null labels])}; packing covers keys (_timeseries or concrete), passthrough labels, and
         * missing labels (null-synthesized for BY over WITHOUT).
         */
        // TODO: TranslateTimeSeriesAggregate independently unpacks the inner TSA's dimensions and we re-pack here; a TSA
        // flag to skip its unpack would eliminate the redundant cycle.
        private LogicalPlan emitPackedRegroup(LogicalPlan plan, ResolvedAttributes translation, NamedExpression value) {
            Source source = cmd.source();
            Attribute step = stepAttr();

            // Null-synthesize missing labels before packing.
            List<Alias> nullAliases = translation.absent().stream().map(TranslatePromqlToEsqlPlan::nullAlias).toList();
            if (nullAliases.isEmpty() == false) {
                plan = new Eval(source, plan, nullAliases);
            }
            List<Attribute> allToPack = new ArrayList<>(translation.groupings());
            allToPack.addAll(translation.passthrough());
            nullAliases.forEach(a -> allToPack.add(a.toAttribute()));

            List<Alias> packAliases = new ArrayList<>();
            List<NamedExpression> packedKeys = new ArrayList<>();
            List<Alias> unpackAliases = new ArrayList<>();
            var names = new TemporaryNameGenerator.Monotonic();
            for (Attribute attr : allToPack) {
                Alias pack = new Alias(source, names.next(attr.name()), new PackDimension(source, attr));
                packAliases.add(pack);
                packedKeys.add(pack.toAttribute());
                var unpack = new UnpackDimension(source, pack.toAttribute(), attr.dataType());
                unpackAliases.add(new Alias(source, attr.name(), unpack, attr.id()));
            }

            Eval packEval = new Eval(source, plan, packAliases);
            Aggregate agg = new Aggregate(source, packEval, groupings(step, packedKeys), aggregates(value, step, packedKeys));
            Eval unpackEval = new Eval(source, agg, unpackAliases);

            List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
            unpackAliases.forEach(u -> projections.add(u.toAttribute()));
            return new Project(source, unpackEval, projections);
        }

        /**
         * BY/NONE over an already-aggregated child: a single {@code Aggregate} grouped by step + concrete labels only;
         * {@code _timeseries} is NOT a grouping key.
         */
        private LogicalPlan emitPlainRegroup(LogicalPlan plan, ResolvedAttributes translation, NamedExpression value) {
            return emitPlainRegroup(plan, translation, value, List.of());
        }

        private LogicalPlan emitPlainRegroup(
            LogicalPlan plan,
            ResolvedAttributes translation,
            NamedExpression value,
            List<Attribute> extraKeys
        ) {
            var keys = new ArrayList<NamedExpression>();
            translation.groupings().stream().filter(k -> isTimeSeriesAttributeName(k.name()) == false).forEach(keys::add);
            keys.addAll(translation.passthrough());
            for (Attribute label : extraKeys) {
                Attribute resolved = findByLabelName(plan.output(), canonicalName(label));
                keys.add(resolved != null ? resolved : label);
            }
            if (translation.absent().isEmpty() == false) {
                List<Alias> missingAliases = new ArrayList<>();
                for (var missing : translation.absent()) {
                    // Surface a passthrough label that exists under its backing field name (labels.X -> X); only truly
                    // absent labels are null-filled. Mirrors Prometheus, where a BY label not present is dropped/empty.
                    var alias = aliasExistingPromqlLabel(plan, missing);
                    missingAliases.add(alias);
                    keys.add(alias.toAttribute());
                }
                plan = new Eval(cmd.source(), plan, missingAliases);
            }

            var step = stepAttr();
            return new Aggregate(cmd.source(), plan, groupings(step, keys), aggregates(value, step, keys));
        }

        private Alias aliasExistingPromqlLabel(LogicalPlan plan, Attribute attribute) {
            Attribute existing = findByLabelName(plan.output(), canonicalName(attribute));
            return existing == null ? nullAlias(attribute) : new Alias(cmd.source(), attribute.name(), existing, attribute.id());
        }

        /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
        private LogicalPlan projectCommandOutput(LogicalPlan plan) {
            var lookupMap = new HashMap<String, Attribute>();
            for (var attr : plan.output()) {
                lookupMap.put(attr.name(), attr);
            }
            var projected = new ArrayList<>(cmd.output());
            var realiased = new ArrayList<Alias>();
            for (int i = 0; i < projected.size(); i++) {
                var attr = projected.get(i);
                var lookupAttr = lookupMap.get(attr.name());
                if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                    var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                    realiased.add(alias);
                    projected.set(i, alias.toAttribute());
                }
            }
            if (realiased.isEmpty() == false) {
                plan = new Eval(cmd.source(), plan, realiased);
            }
            return new Project(cmd.source(), plan, projected);
        }

        /** Keeps only steps within the query range; step labels are anchored at {@code start} and offset-independent. */
        private LogicalPlan filterStepBounds(LogicalPlan plan) {
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
        private Expression sourceTimeFilter(LogicalPlan branch) {
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

        /** Adds an Eval on top of the source relation materializing the evaluation timestamp (@timestamp + offset). */
        private LogicalPlan materializeEvaluationTimestamp(LogicalPlan plan, LogicalPlan branch) {
            if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
                Expression base = cmd.timestamp();
                if (base.dataType() == DataType.DATE_NANOS) {
                    base = new ToDatetime(base.source(), base, configuration());
                }
                var offset = cmd.collectFirstOffsetForBranch(branch);
                var shifted = offset.isZero()
                    ? base
                    : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
                var time = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
                return plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(time)));
            }
            return plan;
        }

        /** Pushes the label filter down to the EsRelation, combining with an existing relation filter. */
        private LogicalPlan pushFilterToRelation(LogicalPlan plan, Expression filterCondition) {
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
        private Alias castValueToDouble(Expression valueExpr, NameId valueId) {
            if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
                valueExpr = new ToDouble(cmd.source(), valueExpr);
            }
            return new Alias(cmd.source(), cmd.valueColumnName(), valueExpr, valueId);
        }

        /**
         * The {@code step} bucket for a branch: the {@link TStep} grouping key shared across all aggregation groupings,
         * derived from the (possibly offset-shifted) evaluation timestamp - so an {@code offset} shifts which samples
         * fall into each fixed output bucket without moving the buckets. {@code stepId} names the synthetic column.
         */
        private Alias stepBucket(NameId stepId, Expression time) {
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
    }

    // -- pure helpers, independent of the running translation --

    private static InheritedAttributes inheritedGrouping(AcrossSeriesAggregate agg, InheritedAttributes scope) {
        return switch (agg.grouping()) {
            case BY -> scope.limitedTo(agg.groupings());
            case WITHOUT -> scope.excluding(agg.groupings());
            case NONE -> InheritedAttributes.unconstrained();
        };
    }

    private static SynthesizedAttributes synthesizedGrouping(AcrossSeriesAggregate agg, IntermediateResult child) {
        return switch (agg.grouping()) {
            case BY -> SynthesizedAttributes.foldIncluding(agg.output(), child.labels());
            case WITHOUT -> SynthesizedAttributes.foldExcluding(agg.groupings(), child.labels());
            case NONE -> SynthesizedAttributes.none();
        };
    }

    /**
     * Ensure {@code _timeseries} survives in the exported labels: without `le` no {@link TimeSeriesWithout} is inserted
     * and concrete-dimension grouping drops {@code _timeseries} from the output, yet the command still projects it.
     */
    private static SynthesizedAttributes preserveTimeseries(SynthesizedAttributes labels, List<Attribute> childOutput) {
        Attribute ts = PromqlAttributesTranslationContext.findByFieldName(childOutput, MetadataAttribute.TIMESERIES);
        if (ts != null && PromqlAttributesTranslationContext.findByFieldName(labels.declared(), MetadataAttribute.TIMESERIES) == null) {
            return SynthesizedAttributes.of(PromqlAttributesTranslationContext.union(labels.declared(), List.of(ts)));
        }
        return labels;
    }

    /** The label shape a freshly materialized child exposes: its concrete dimensions plus {@code _timeseries} if present. */
    private static SynthesizedAttributes synthesizedLabels(LogicalPlan plan) {
        List<Attribute> labels = concreteDimensions(plan.output());
        Attribute ts = PromqlAttributesTranslationContext.findByFieldName(plan.output(), MetadataAttribute.TIMESERIES);
        return SynthesizedAttributes.of(ts != null ? PromqlAttributesTranslationContext.union(labels, List.of(ts)) : labels);
    }

    /** Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    /** The uniform aggregation shape: group by step + keys, output {@code [value, step, keys...]}. */
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

    private static Alias nullAlias(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /** The first output attribute is always the value column. */
    private static Expression valueColumn(LogicalPlan plan) {
        return plan.output().getFirst().toAttribute();
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan dropNullRows(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    private static boolean isImplicitRangePlaceholder(Expression range) {
        return range.foldable()
            && range.fold(FoldContext.small()) instanceof Duration duration
            && duration.equals(PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER);
    }

    /**
     * Lowers PromQL label matchers into an AND of per-label ESQL predicates. Uses {@link AutomatonUtils} to lower a
     * pattern to a predicate cheaper than a regex where possible: exact values become equality/IN, prefix/suffix
     * alternations become STARTS_WITH/ENDS_WITH disjunctions, everything else falls back to RLIKE.
     */
    private static Expression lowerMatchers(Source source, List<Expression> fields, LabelMatchers labelMatchers, Configuration config) {
        var matchers = labelMatchers.matchers();
        List<Expression> conditions = new ArrayList<>(matchers.size());
        boolean hasNameMatcher = false;
        for (int i = 0, s = matchers.size(); i < s; i++) {
            LabelMatcher matcher = matchers.get(i);
            // the metric name matcher selects the series; it has no label field to filter on
            if (LabelMatcher.NAME.equals(matcher.name())) {
                hasNameMatcher = true;
                continue;
            }
            Expression field = fields.get(hasNameMatcher ? i - 1 : i); // adjust index if name matcher was seen
            if (field.resolved() && DataType.isString(field.dataType()) == false) {
                field = new ToString(field.source(), field, config);
            }
            conditions.add(translateLabelMatcher(source, field, matcher));
        }
        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    /** Lowers a single PromQL label matcher to an ESQL predicate; public API also used by the prometheus REST layer. */
    public static Expression translateLabelMatcher(Source source, Expression field, LabelMatcher matcher) {
        if (matcher.matchesAll()) {
            return Literal.fromBoolean(source, true);
        }
        if (matcher.matchesNone()) {
            return Literal.fromBoolean(source, false);
        }
        Expression condition;
        if (matcher.isMultiValue()) {
            // each value is a regex, combine with OR; plain literals match exact with an IN clause
            condition = matcher.matcher().isRegex()
                ? Predicates.combineOr(
                    matcher.values().stream().<Expression>map(v -> new RLike(source, field, new RLikePattern(v))).toList()
                )
                : new In(source, field, matcher.values().stream().<Expression>map(v -> Literal.keyword(source, v)).toList());
            if (matcher.isNegation()) {
                condition = new Not(source, condition);
            }
        } else {
            var exact = AutomatonUtils.matchesExact(matcher.automaton());
            if (exact != null) {
                condition = new Equals(source, field, Literal.keyword(source, exact));
            } else {
                var fragments = AutomatonUtils.extractFragments(matcher.getFirstValue());
                condition = fragments != null && fragments.isEmpty() == false
                    ? translateDisjointPatterns(source, field, fragments)
                    // fallback: RLIKE over the full pattern, anchored per PromQL semantics
                    : new RLike(source, field, new RLikePattern(matcher.getFirstValue()));
                if (matcher.isNegation()) {
                    condition = new Not(source, condition);
                }
            }
        }
        // absent labels are treated as having value "" because if the matcher accepts the empty string
        // (e.g. {label=""} or {label!="foo"}), series where the label field is NULL (absent) must also match.
        if (matcher.matchesEmpty()) {
            condition = Predicates.combineOr(List.of(new IsNull(source, field), condition));
        }
        return condition;
    }

    /** Disjoint fragments sort EXACT -> PREFIX -> SUFFIX -> REGEX (most selective first); an all-EXACT set lowers to IN. */
    private static Expression translateDisjointPatterns(Source source, Expression field, List<AutomatonUtils.PatternFragment> fragments) {
        var sorted = fragments.stream().sorted(Comparator.comparingInt(f -> f.type().ordinal())).toList();
        if (sorted.stream().allMatch(f -> f.type() == AutomatonUtils.PatternFragment.Type.EXACT)) {
            return new In(source, field, sorted.stream().<Expression>map(f -> Literal.keyword(source, f.value())).toList());
        }
        return Predicates.combineOr(sorted.stream().map(f -> translatePatternFragment(source, field, f)).toList());
    }

    private static Expression translatePatternFragment(Source source, Expression field, AutomatonUtils.PatternFragment fragment) {
        Literal value = Literal.keyword(source, fragment.value());
        return switch (fragment.type()) {
            case EXACT -> new Equals(source, field, value);
            case PREFIX -> new StartsWith(source, field, value);
            case PROPER_PREFIX -> new And(source, new NotEquals(source, field, value), new StartsWith(source, field, value));
            case SUFFIX -> new EndsWith(source, field, value);
            case PROPER_SUFFIX -> new And(source, new NotEquals(source, field, value), new EndsWith(source, field, value));
            case REGEX -> new RLike(source, field, new RLikePattern(fragment.value()));
        };
    }
}
