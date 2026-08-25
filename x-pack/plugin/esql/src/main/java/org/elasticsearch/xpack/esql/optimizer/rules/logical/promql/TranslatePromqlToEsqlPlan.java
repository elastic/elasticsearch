/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
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
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.NamedColumn;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.TimeSeriesColumn;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
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

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.findById;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.findByIdOrName;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.findByName;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.resolveColumn;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.toCanonicalName;
import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;

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

    /** The lifecycle of an intermediate result. A constant is always a finished (aggregation-free) local relation. */
    private enum Kind {
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
        Header header,
        /* The translator tracks what it built instead of inspecting the plan. */
        Kind kind
    ) {
        IntermediateResult {
            if (kind.afterInitialAggregation) {
                header = header.transformExpressions((column, grouping) -> resolveColumn(column, plan.output()));
            }
        }

        IntermediateResult(LogicalPlan plan, Expression value) {
            this(plan, value, null, Header.undefined(), Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Expression selectorFilter) {
            this(plan, value, selectorFilter, Header.undefined(), Kind.BEFORE_INITIAL_AGGREGATE);
        }

        IntermediateResult(LogicalPlan plan, Expression value, Expression selectorFilter, Header header) {
            this(plan, value, selectorFilter, header, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan/value, keeping its other properties. */
        IntermediateResult with(LogicalPlan plan, Expression value, Header header) {
            return new IntermediateResult(plan, value, pendingFilter, header, kind);
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
        Translation translation = new Translation(cmd, context, null, Header.undefined(), null);
        return translation.translateFinal();
    }

    /**
     * One translation pass: the command (or an operand fork of it), the analyzer context, and the state of the translateIntermediate
     * being compiled. Independent parts compile separately - like modules - each with its own instance: a narrowed
     * required header is {@link #withPushDownHeader}, and a union branch translateIntermediate is a fresh instance with its own
     * step bucket and evaluation time.
     */
    private record Translation(
        PromqlCommand cmd,
        AnalyzerContext analyzer,
        /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
        Alias stepBucketAlias,
        /* The header the result subtree MUST produce. */
        Header headerToPushDown,
        /* The current translateIntermediate evaluation time (default: @timestamp). */
        Expression time
    ) {
        Configuration configuration() {
            return analyzer.configuration();
        }

        Attribute stepAttr() {
            return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
        }

        Translation withPushDownHeader(Header pushDownHeader) {
            return new Translation(cmd, analyzer, stepBucketAlias, pushDownHeader, time);
        }

        /** Translates one union branch with its own step bucket and evaluation time. */
        IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
            Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
            Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
            var run = new Translation(cmd, analyzer, step, headerToPushDown, branchTime);
            return run.translateIntermediate(branch, valueId);
        }

        LogicalPlan translateFinal() {
            // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
            // cannot compose as a single-value sub-expression.
            // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
            var branches = new ArrayList<LogicalPlan>();
            flattenUnion(cmd.promqlPlan(), branches);

            if (branches.size() == 1) {
                IntermediateResult intermediateResult = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
                return doTranslateFinal(intermediateResult.plan(), intermediateResult.kind.constant);
            }
            // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
            var intermediateResultPlan = doTranslateUnion(
                branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
            );
            return doTranslateFinal(intermediateResultPlan, false);
        }

        // -- helpers --

        /* Shared by every `final` translation root */
        private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation) {
            plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan), cmd.valueAttribute());
            return localRelation ? plan : emitByStepFilter(plan);
        }

        /**
         * Union combinator over independently translated tabular results.
         * {@link UnionAll} aligns columns by name and null-fills missing header, then
         * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
         */
        private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
            // Already validated against Fork.MAX_BRANCHES by PromqlCommand.verify
            assert Fork.exceedsMaxBranches(intermediateResults.size()) == false
                : "invariant: union branch count ["
                    + intermediateResults.size()
                    + "] must be less of equal Fork.MAX_BRANCHES ["
                    + Fork.MAX_BRANCHES
                    + "]";

            var source = cmd.source();
            var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
            for (int i = 0; i < intermediateResults.size(); i++) {
                // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
                var ir = intermediateResults.get(i);
                LogicalPlan branchPlan = emitNullsFilter(source, ir.plan(), ir.valueColumn());
                var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
                branchPlans.add(new Eval(source, branchPlan, List.of(branchTagExpression)));
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

        /**
         * Translates independent query fragment into intermediate result (IR).
         * Think of IR as table
         */
        private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
            IntermediateResult ir = doTranslateTryInline(doTranslateNode(branch));

            var plan = ir.plan();
            var valueExpr = ir.value();
            var header = ir.header();
            Expression timeFilter = emitBySrcTimeFilter(branch);
            var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
            if (filter != null) {
                plan = pushDownSrcTimestampFilter(plan, filter);
            }

            if (ir.kind.constant == false) {
                // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
                // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
                if (ir.kind.afterInitialAggregation == false) {
                    plan = emitInitialAggregate(plan, header, valueExpr);
                    valueExpr = collectValueAttribute(plan);
                }
                if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    ToDouble right = new ToDouble(comparison.right().source(), ((LiteralSelector) comparison.right()).literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), valueExpr, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }

            // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
            Alias value = emitValueDoubleCastExpression(valueExpr, valueId);
            plan = new Eval(cmd.source(), plan, List.of(value));
            if (ir.kind.constant == false) {
                plan = pushDownEvaluationTimestampFilter(plan, branch);
            }

            Kind kind = ir.kind.constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
            return new IntermediateResult(plan, value.toAttribute(), null, header, kind);
        }

        /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
        private IntermediateResult doTranslateTryInline(IntermediateResult result) {
            Attribute stepAttr = cmd.stepAttribute();
            if (result.kind.constant
                || cmd.start().value() == null
                || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
                return result;
            }
            var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
            var step = plan.output().getFirst();
            var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
            return new IntermediateResult(plan, value, result.pendingFilter(), result.header(), Kind.CONSTANT);
        }

        /**
         * Recursively translates a PromQL plan node. The source relation {@code cmd.child()} is the leaf at the bottom of
         * the produced subtree; the PromQL tree is walked top-down and the ESQL plan assembled bottom-up on the way back.
         */
        private IntermediateResult doTranslateNode(LogicalPlan node) {
            return switch (node) {
                case AcrossSeriesAggregate agg -> doTranslateAcrossSeriesAgg(agg);
                case AcrossSeriesReduction reduction -> doTranslateAcrossSeriesReduction(reduction);
                case HistogramFunctionCall histogramFunction -> doTranslateHistogramFunction(histogramFunction);
                case ScalarConversionFunction scalar -> doTranslateScalarConvertion(scalar);
                case MetadataManipulationFunction relabel -> doTranslateMetadataManipulation(relabel);
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
        private IntermediateResult doTranslateAddValueEval(IntermediateResult t, Expression value, Header header) {
            if (t.kind.afterInitialAggregation == false) {
                return t.with(t.plan(), value, header);
            }
            Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
            return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute(), header);
        }

        /**
         * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}. PromQL aggregation shape is dynamic and
         * cannot be enumerated at plan time. A parent translates its child, inspects the returned shape, and re-invokes
         * it with an additional load-time identity requirement when necessary. Only {@code AcrossSeriesAggregate}
         * creates plan-level aggregation nodes; within-series aggregates and function calls lower to expressions.
         */
        private IntermediateResult doTranslateAcrossSeriesAgg(AcrossSeriesAggregate agg) {
            var maybeChildHeader = headerToPushDown.withAcrossSeriesAgg(agg.grouping(), agg.groupings());
            var fistPhaseRun = withPushDownHeader(maybeChildHeader);
            IntermediateResult ir = fistPhaseRun.doTranslateNode(agg.child());
            if (ir.kind.constant) {
                return ir;
            }

            // check if push-down successful
            var childHeader = ir.header;
            var currHeader = childHeader.withAcrossSeriesAgg(agg.grouping(), agg.groupings(), agg.output());
            maybeChildHeader = maybeChildHeader.requiring(currHeader);
            if (maybeChildHeader.success(childHeader) == false) {
                var secondPhaseRun = withPushDownHeader(maybeChildHeader);
                ir = secondPhaseRun.doTranslateNode(agg.child());
                currHeader = ir.header().withAcrossSeriesAgg(agg.grouping(), agg.groupings(), agg.output());
                maybeChildHeader = maybeChildHeader.requiring(currHeader);
                assert maybeChildHeader.success(ir.header)
                    : "invariant: header [" + maybeChildHeader + "] expected, got [" + ir.header + "]";
            }
            Header outputHeader = ir.header().regrouped(currHeader, this.headerToPushDown);

            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, stepAttr(), configuration());
            return doTranslateAgg(ir, ir.plan(), outputHeader, agg.grouping() == WITHOUT, agg.buildEsqlFunction(ir.value(), promqlCtx));
        }

        /**
         * Translates an {@link AcrossSeriesReduction} ({@code topk}/{@code bottomk}): collapse the child to one row
         * per series, then rank and keep the top {@code k}. A {@code by} clause only partitions the ranking; it does
         * not change output header.
         */
        private IntermediateResult doTranslateAcrossSeriesReduction(AcrossSeriesReduction plan) {
            if (plan.grouping() == WITHOUT) {
                throw new VerificationException("function [{}] is not yet supported with [{}]", plan.functionName(), WITHOUT.name());
            }

            Translation childTranslation = withPushDownHeader(Header.undefined());
            IntermediateResult childResult = childTranslation.doTranslateNode(plan.child());
            if (childResult.kind.constant) {
                return childResult;
            }

            var header = childResult.header().including(plan.groupings());

            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, stepAttr(), configuration());
            IntermediateResult aggregated = doTranslateAgg(childResult, childResult.plan(), header, false, childResult.value());
            LogicalPlan result = emitTopNBy(plan, aggregated.plan(), header, promqlCtx);
            return aggregated.with(result, aggregated.value(), header);
        }

        /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step. */
        private LogicalPlan emitTopNBy(
            AcrossSeriesReduction reduction,
            LogicalPlan resultPlan,
            Header header,
            PromqlContext promqlContext
        ) {
            var groupings = new ArrayList<Expression>();
            groupings.add(stepAttr());
            if (reduction.grouping() == AcrossSeriesAggregate.Grouping.BY) {
                header = header.transformExpressions((column, grouping) -> resolveColumn(column, resultPlan.output()));
                for (var label : reduction.groupings()) {
                    Attribute resolved = header.column(toCanonicalName(label));
                    assert resolved != null : "invariant: [ " + reduction.functionName() + " ] requre a partition label [ " + label + " ]";
                    groupings.add(resolved);
                }
            }
            var order = (Order) reduction.buildEsqlFunction(collectValueAttribute(resultPlan), promqlContext);
            return new TopNBy(
                reduction.source(),
                resultPlan,
                order != null ? List.of(order) : List.<Order>of(),
                new ToInteger(reduction.source(), reduction.parameters().getFirst()),
                groupings
            );
        }

        /** The doTranslateAgg combinator: regroups a grouped table, or emits the innermost `_timeseries` doTranslateAgg over a raw one. */
        private IntermediateResult doTranslateAgg(IntermediateResult child, LogicalPlan plan, Header header, boolean pack, Expression agg) {
            LogicalPlan result;
            if (child.kind.afterInitialAggregation) {
                result = emitIntermediateAggregate(plan, header, agg, header.hasTimeSeriesGrouping() || pack);
            } else {
                result = emitInitialAggregate(plan, header, agg);
            }
            return new IntermediateResult(
                result,
                collectValueAttribute(result),
                child.pendingFilter(),
                header,
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }

        private IntermediateResult doTranslateHistogramFunction(HistogramFunctionCall function) {
            IntermediateResult firstPhaseResult = doTranslateNode(function.child());
            if (firstPhaseResult.kind.constant) {
                return firstPhaseResult;
            }

            // native histograms - distinguishable only at this point in planning are regular value transformations.
            if (firstPhaseResult.value().resolved() && firstPhaseResult.value().dataType().isHistogram()) {
                return doTranslateFunc(
                    new ValueTransformationFunction(function.source(), function.child(), function.definition(), function.parameters())
                );
            }

            // Classic counter-backed histograms need the special treatment below.
            LogicalPlan childPlan = firstPhaseResult.plan();
            var le = firstPhaseResult.header().column(HistogramFunctionCall.LE_LABEL);
            if (le == null) {
                // like prometheus, return warning and drop series w/o `le`
                HeaderWarning.addWarning(function.functionName() + ": input vector has no le label; no buckets to evaluate");
                var skipAllFilter = new Filter(function.source(), childPlan, Literal.FALSE);
                var nullGrouping = new Values(function.source(), new Literal(function.source(), null, DataType.DOUBLE));
                return doTranslateAgg(firstPhaseResult, skipAllFilter, firstPhaseResult.header(), false, nullGrouping);
            }

            // `le` exists on child plan -> group by `le`
            Header grouping = firstPhaseResult.header().groupedWithout(List.of(le));
            Header maybeChildHeader = headerToPushDown.requiring(grouping);

            // check if our parent push-down labels on child plan
            if (maybeChildHeader.success(firstPhaseResult.header()) == false) {
                // in addition to `le` preserve our parent push-down labels
                Translation childTranslation = withPushDownHeader(maybeChildHeader);
                var secondPhaseResult = childTranslation.doTranslateNode(function.child());
                childPlan = secondPhaseResult.plan();
                le = secondPhaseResult.header().column(HistogramFunctionCall.LE_LABEL);

                assert le != null : "invariant: [ " + HistogramFunctionCall.LE_LABEL + " ] required";

                // ?
                grouping = secondPhaseResult.header().groupedWithout(List.of(le));
                maybeChildHeader = maybeChildHeader.requiring(grouping);

                firstPhaseResult = secondPhaseResult;

                assert maybeChildHeader.success(secondPhaseResult.header())
                    : "invariant: [ " + HistogramFunctionCall.LE_LABEL + " ] required";
            }

            if (firstPhaseResult.kind.afterInitialAggregation == false) {
                childPlan = emitInitialAggregate(childPlan, firstPhaseResult.header(), firstPhaseResult.value());
                firstPhaseResult = new IntermediateResult(
                    childPlan,
                    collectValueAttribute(childPlan),
                    firstPhaseResult.pendingFilter(),
                    firstPhaseResult.header(),
                    Kind.AFTER_INITIAL_AGGREGATE
                );
                le = firstPhaseResult.header().column(HistogramFunctionCall.LE_LABEL);
                assert le != null : "invariant: [ " + HistogramFunctionCall.LE_LABEL + " ] required";
            }

            // Classic histogram functions group by every label except the `le` bucket label, so `le` is the single excluded
            // dimension - the returned header drops it and the innermost `_timeseries` excludes it. Bucket counts are
            // consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
            Header header = firstPhaseResult.header().regrouped(grouping, headerToPushDown);
            Expression count = new ToDouble(function.source(), firstPhaseResult.value());
            return doTranslateAgg(firstPhaseResult, childPlan, header, true, function.buildAggregateFunction(count, le));
        }

        /** scalar(): collapse to one value per step, e.g. scalar(sum by (cluster) (metric)). */
        private IntermediateResult doTranslateScalarConvertion(ScalarConversionFunction scalarFunc) {
            IntermediateResult child = doTranslateNode(scalarFunc.child());
            if (child.value().foldable()) {
                return new IntermediateResult(child.plan(), new ToDouble(scalarFunc.source(), child.value()), child.pendingFilter());
            }
            var scalarExpr = new Scalar(scalarFunc.source(), child.value());
            return doTranslateAgg(child, child.plan(), Header.undefined(), false, scalarExpr);
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
            return doTranslateAddValueEval(child, functionCall.buildEsqlFunction(child.value(), promqlCtx), child.header());
        }

        /**
         * Translates a {@code label_replace}/{@code label_join} into a derived label column.
         * <p>
         * The child is first collapsed to one row per series (forcing the initial aggregate when it has not happened yet), so
         * the source labels are materialized as columns to derive from. The destination value is then computed with an
         * {@link Eval} under the destination's stable id, and exposed as a named column so the enclosing {@code by(...)}
         * aggregation groups on it exactly as it would on a stored label.
         * <p>
         * Because ES|QL treats {@code null} and {@code ""} as distinct grouping keys while Prometheus treats an absent label
         * and an empty label value alike, every "label absent" outcome is normalized to {@code ""}: an absent source is
         * coalesced to {@code ""} before matching, and {@code label_replace}'s no-match ({@code null}) is coalesced back to
         * {@code ""}. All such series therefore fall into the same group, matching Prometheus.
         */
        private IntermediateResult doTranslateMetadataManipulation(MetadataManipulationFunction relabel) {
            IntermediateResult child = doTranslateNode(relabel.child());
            if (child.kind.constant) {
                return child;
            }

            // Collapse to one row per series so the source labels exist as columns; this mirrors the seam in
            // translateIntermediate that forces the initial per-series aggregate for a not-yet-aggregated subtree.
            IntermediateResult aggregated = child;
            if (child.kind.afterInitialAggregation == false) {
                LogicalPlan plan = emitInitialAggregate(child.plan(), child.header(), child.value());
                Expression value = collectValueAttribute(plan);
                aggregated = new IntermediateResult(plan, value, child.pendingFilter(), child.header(), Kind.AFTER_INITIAL_AGGREGATE);
            }

            Source source = relabel.source();
            Attribute destination = relabel.destination();
            Expression destinationValue = relabel.definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
                ? labelReplaceValue(source, relabel, aggregated.header())
                : labelJoinValue(source, relabel, aggregated.header());

            Alias derived = new Alias(source, destination.name(), destinationValue, destination.id());
            LogicalPlan withDerived = new Eval(cmd.source(), aggregated.plan(), List.of(derived));
            Header header = aggregated.header().shadowing(List.of(derived.toAttribute()));
            return new IntermediateResult(
                withDerived,
                aggregated.value(),
                aggregated.pendingFilter(),
                header,
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }

        /**
         * The {@code label_replace} destination value:
         * {@code COALESCE(RegexExpand(COALESCE(src, ""), regex, repl), existingDst)}.
         * The inner coalesce feeds the empty string when the source label is absent (so the regex matches against {@code ""}
         * like Prometheus). The outer coalesce implements Prometheus's no-match semantics: a no-match ({@code null}) leaves
         * the destination label unchanged, so it falls back to the destination's existing value - the stored label when the
         * destination overwrites one, or {@code ""} (the "absent" grouping key) when the destination is a new label. A match
         * with an empty expansion (the delete sentinel) resolves to {@code ""}, joining that same "absent" group.
         */
        private Expression labelReplaceValue(Source source, MetadataManipulationFunction relabel, Header header) {
            List<Expression> params = relabel.parameters();
            Attribute destination = relabel.destination();
            String srcLabel = literalString(params.get(2));
            Expression regex = params.get(3);
            Expression replacement = params.get(1);
            Expression src = sourceLabelValue(source, header, srcLabel, destination);
            Expression extracted = new RegexExpand(source, src, regex, replacement);
            Expression existingDst = sourceLabelValue(source, header, destination.name(), destination);
            return new Coalesce(source, extracted, List.of(existingDst));
        }

        /**
         * The {@code label_join} destination value: the source label values coalesced to {@code ""} and joined by the
         * separator. With no source labels the result is {@code ""} - the same "absent" grouping key produced by
         * {@code label_replace}; a single source label is copied verbatim (no separator). With two or more source labels the
         * separator is inserted between every value, so even all-empty sources yield the separator run (for example a
         * {@code "-"} separator over two absent labels produces {@code "-"}), matching Prometheus.
         */
        private Expression labelJoinValue(Source source, MetadataManipulationFunction relabel, Header header) {
            List<Expression> params = relabel.parameters();
            Attribute destination = relabel.destination();
            String separator = literalString(params.get(1));
            List<Expression> sources = params.subList(2, params.size())
                .stream()
                .map(param -> sourceLabelValue(source, header, literalString(param), destination))
                .toList();
            if (sources.isEmpty()) {
                return Literal.keyword(source, "");
            }
            if (sources.size() == 1) {
                return sources.getFirst();
            }
            List<Expression> parts = new ArrayList<>(sources.size() * 2 - 1);
            for (int i = 0; i < sources.size(); i++) {
                if (i > 0) {
                    parts.add(Literal.keyword(source, separator));
                }
                parts.add(sources.get(i));
            }
            return new Concat(source, parts.getFirst(), parts.subList(1, parts.size()));
        }

        /**
         * The value of a source label as a non-null string: {@code COALESCE(ToString(label), "")}, or {@code ""} if the
         * label is absent. {@code destination} is the relabel's derived destination proxy, excluded from the lookup so a
         * label reference never resolves to the destination that an enclosing {@code by(dst)} pushed down under the same
         * name (which would create a self-reference).
         */
        private Expression sourceLabelValue(Source source, Header header, String labelName, Attribute destination) {
            Attribute label = header.column(labelName, destination);
            if (label == null) {
                return Literal.keyword(source, "");
            }
            Expression stringValue = DataType.isString(label.dataType()) ? label : new ToString(source, label, configuration());
            return new Coalesce(source, stringValue, List.of(Literal.keyword(source, "")));
        }

        /** Translates a scalar function (time(), etc.): an expression over the unchanged source. */
        private IntermediateResult doTranslateScalarFunc(ScalarFunction scalarFunction) {
            var function = scalarFunction.buildEsqlFunction(new PromqlContext(cmd.timestamp(), null, cmd.stepAttribute(), configuration()));
            return new IntermediateResult(cmd.child(), function);
        }

        /** Translates binary operators by composing the operator as an expression over a shared frame. */
        private IntermediateResult doTranslateBinaryOp(VectorBinaryOperator binaryOp) {
            IntermediateResult left = doTranslateNode(binaryOp.left());
            Expression leftExpr = new ToDouble(left.value().source(), left.value());
            if (binaryOp instanceof VectorBinaryComparison comp && comp.filterMode()) {
                return left.with(left.plan(), leftExpr, left.header());
            }

            IntermediateResult right = doTranslateNode(binaryOp.right());
            Expression rightExpr = new ToDouble(right.value().source(), right.value());
            Expression binaryExpr = binaryOp.binaryOp().asFunction().create(binaryOp.source(), leftExpr, rightExpr, configuration());

            LogicalPlan plan;
            Expression filter;
            if (left.kind.afterInitialAggregation && right.kind.afterInitialAggregation) {
                plan = emitBinaryOperatorAggregateExpression(left, right);
                filter = null;
            } else {
                plan = left.kind.afterInitialAggregation ? left.plan() : right.plan();
                filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
            }
            Header shape = left.header().isDefined() ? left.header() : right.header();
            Kind kind = left.kind.afterInitialAggregation || right.kind.afterInitialAggregation
                ? Kind.AFTER_INITIAL_AGGREGATE
                : Kind.BEFORE_INITIAL_AGGREGATE;
            IntermediateResult result = new IntermediateResult(plan, null, filter, shape, kind);
            return doTranslateAddValueEval(result, binaryExpr, shape);
        }

        /** Fold left and right aggregates into a single plan. */
        private LogicalPlan emitBinaryOperatorAggregateExpression(IntermediateResult left, IntermediateResult right) {
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
                        "binary operations between vectors with mismatched grouping keys are not yet supported"
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
            Expression matcher = emitMatchersPredicateExpression(
                selector.source(),
                selector.labels(),
                selector.labelMatchers(),
                configuration()
            );

            if (selector instanceof LiteralSelector literalSelector) {
                return foldedPlan != null
                    ? new IntermediateResult(foldedPlan, literalSelector.literal(), matcher, Header.undefined(), Kind.CONSTANT)
                    : new IntermediateResult(input, literalSelector.literal(), matcher, Header.undefined());
            }
            if (foldedPlan != null) {
                var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
                return new IntermediateResult(empty, Literal.NULL, null, Header.undefined(), Kind.CONSTANT);
            }

            // An instant selector maps to LastOverTime to get the latest sample per time series.
            Expression expr = selector instanceof InstantSelector
                ? new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, time)
                : selector.series();
            List<Attribute> dimensions = input.output()
                .stream()
                .filter(attribute -> attribute instanceof FieldAttribute field && field.isDimension())
                .filter(attribute -> attribute instanceof TimeSeriesMetadataAttribute == false)
                .toList();
            return new IntermediateResult(input, expr, matcher, headerToPushDown.withIdentityGrouping().including(dimensions));
        }

        /**
         * The innermost doTranslateAgg owns the physical {@code _timeseries} grouping and materializes every ephemeral
         * column in the header with that column's own exclusions.
         */
        private LogicalPlan emitInitialAggregate(LogicalPlan plan, Header header, Expression agg) {
            Source source = cmd.promqlPlan().source();
            boolean needsTimeSeriesGrouping = header.hasTimeSeriesColumns();
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

            var names = new TemporaryNameGenerator.Monotonic();
            Header physicalHeader = header.transformExpressions((col, grouping) -> {
                if (col instanceof TimeSeriesColumn tc) {
                    List<Expression> excluded = tc.exclusions().stream().<Expression>map(label -> {
                        Attribute resolved = findByName(plan.output(), toCanonicalName(label));
                        return resolved != null ? resolved : label;
                    }).toList();
                    String name = grouping ? MetadataAttribute.TIMESERIES : names.next(MetadataAttribute.TIMESERIES);
                    Alias alias = new Alias(source, name, new TimeSeriesWithout(source, excluded), tc.attribute().id());
                    return new TimeSeriesColumn(alias, tc.exclusions());
                }
                var m = findByIdOrName(col.attribute(), plan.output());
                return m != null ? new NamedColumn(m) : null;
            });
            // Every exposed column is functionally dependent on the series identity, so grouping by all of them preserves
            // per-series granularity while making the full transformed header available to the surrounding query.
            List<NamedExpression> groupKeys = physicalHeader.expressions();
            List<NamedExpression> outKeys = physicalHeader.exposedExpressions();

            var value = new Alias(agg.source(), cmd.valueColumnName(), agg);
            return new TimeSeriesAggregate(
                source,
                plan,
                groupings(stepBucketAlias, groupKeys),
                aggregates(value, stepAttr(), outKeys),
                null,
                time,
                TimeSeriesAggregate.Origin.PROMQL_COMMAND
            );
        }

        /**
         * Regroups an already-aggregated child. Every regroup first resolves its physical header and null-fills missing
         * grouping columns. A WITHOUT regroup additionally packs dimensions before aggregation to prevent multi-valued
         * dimensions from splitting rows and double-counting, then unpacks them afterwards.
         */
        private LogicalPlan emitIntermediateAggregate(LogicalPlan plan, Header header, Expression aggExpr, boolean requiresPacking) {
            Source source = cmd.source();
            Attribute step = stepAttr();
            if (aggExpr instanceof AggregateFunction == false) {
                aggExpr = new Values(aggExpr.source(), aggExpr);
            }
            NamedExpression value = new Alias(aggExpr.source(), cmd.valueColumnName(), aggExpr);
            List<Attribute> available = plan.output();

            var nulls = new ArrayList<Alias>();
            Header physicalHeader = header.transformExpressions((col, grouping) -> {
                if (col instanceof TimeSeriesColumn tc) {
                    var m = findById(tc.attribute(), available);
                    return m != null ? new TimeSeriesColumn(m, tc.exclusions()) : null;
                }
                var m = findByIdOrName(col.attribute(), available);
                if (m == null && grouping) {
                    nulls.add(emitNullExpression(col.attribute()));
                    return new NamedColumn(nulls.getLast());
                }
                return m != null ? new NamedColumn(m) : null;
            });

            if (nulls.isEmpty() == false) {
                plan = new Eval(source, plan, nulls);
            }

            if (requiresPacking) {
                // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
                List<Attribute> dims = physicalHeader.exposedExpressions().stream().map(NamedExpression::toAttribute).toList();
                if (dims.isEmpty()) {
                    return new Aggregate(source, plan, groupings(step, List.of()), aggregates(value, step, List.of()));
                }
                Attribute packed = PackDims.newPackedAttribute(source);
                PackDims packDims = new PackDims(source, plan, dims, packed);
                Alias packedGrouping = PackDims.newPackedGrouping(source, packed);
                Aggregate agg = new Aggregate(
                    source,
                    packDims,
                    groupings(step, List.of(packedGrouping)),
                    aggregates(value, step, List.of(packedGrouping.toAttribute()))
                );
                Header unpackedHeader = physicalHeader.transformExpressions((col, grouping) -> {
                    Attribute dim = col.attribute();
                    String name = grouping && col instanceof TimeSeriesColumn ? MetadataAttribute.TIMESERIES : dim.name();
                    var unpacked = new ReferenceAttribute(
                        dim.source(),
                        null,
                        name,
                        dim.dataType().noText(),
                        Nullability.TRUE,
                        dim.id(),
                        false
                    );
                    return col instanceof TimeSeriesColumn tc ? new TimeSeriesColumn(unpacked, tc.exclusions()) : new NamedColumn(unpacked);
                });
                List<Attribute> unpackedDims = unpackedHeader.exposedExpressions().stream().map(NamedExpression::toAttribute).toList();
                UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
                List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
                projections.addAll(unpackedDims);
                return new Project(source, unpackDims, projections);
            } else {
                List<NamedExpression> keys = physicalHeader.exposedExpressions();
                return new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
            }
        }

        /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
        private LogicalPlan emitFinalProjection(LogicalPlan plan) {
            var lookupMap = new HashMap<String, Attribute>();
            for (var attr : plan.output()) {
                lookupMap.put(attr.name(), attr);
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

        /** Keeps only steps within the query range; step header are anchored at {@code start} and offset-independent. */
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

        /** Adds an Eval on top of the source relation materializing the evaluation timestamp (@timestamp + offset). */
        private LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
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
            return new Alias(cmd.source(), cmd.valueColumnName(), valueExpr, valueId);
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
    }

    // -- pure helpers, independent of the running translation --

    /** Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
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

    private static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /** The first output attribute is always the value column. */
    private static Expression collectValueAttribute(LogicalPlan plan) {
        return plan.output().getFirst().toAttribute();
    }

    /** The string value of a keyword-literal PromQL function argument. */
    private static String literalString(Expression literal) {
        return BytesRefs.toString(((Literal) literal).value());
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
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
    private static Expression emitMatchersPredicateExpression(
        Source source,
        List<Expression> fields,
        LabelMatchers labelMatchers,
        Configuration config
    ) {
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
            conditions.add(emitMatcherConditionExpression(source, field, matcher));
        }
        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    /** Lowers a single PromQL label matcher to an ESQL predicate; public API also used by the prometheus REST layer. */
    public static Expression emitMatcherConditionExpression(Source source, Expression field, LabelMatcher matcher) {
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
                    ? emitMatcherOperatorFn(source, field, fragments)
                    // fallback: RLIKE over the full pattern, anchored per PromQL semantics
                    : new RLike(source, field, new RLikePattern(matcher.getFirstValue()));
                if (matcher.isNegation()) {
                    condition = new Not(source, condition);
                }
            }
        }
        // absent header are treated as having value "" because if the matcher accepts the empty string
        // (e.g. {label=""} or {label!="foo"}), series where the label field is NULL (absent) must also match.
        if (matcher.matchesEmpty()) {
            condition = Predicates.combineOr(List.of(new IsNull(source, field), condition));
        }
        return condition;
    }

    /** Disjoint fragments sort EXACT -> PREFIX -> SUFFIX -> REGEX (most selective first); an all-EXACT set lowers to IN. */
    private static Expression emitMatcherOperatorFn(Source source, Expression field, List<AutomatonUtils.PatternFragment> fragments) {
        var sorted = fragments.stream().sorted(Comparator.comparingInt(f -> f.type().ordinal())).toList();
        if (sorted.stream().allMatch(f -> f.type() == AutomatonUtils.PatternFragment.Type.EXACT)) {
            return new In(source, field, sorted.stream().<Expression>map(f -> Literal.keyword(source, f.value())).toList());
        }

        var expr = sorted.stream().map(f -> {
            Literal value = Literal.keyword(source, f.value());
            return switch (f.type()) {
                case EXACT -> new Equals(source, field, value);
                case PREFIX -> new StartsWith(source, field, value);
                case PROPER_PREFIX -> new And(source, new NotEquals(source, field, value), new StartsWith(source, field, value));
                case SUFFIX -> new EndsWith(source, field, value);
                case PROPER_SUFFIX -> new And(source, new NotEquals(source, field, value), new EndsWith(source, field, value));
                case REGEX -> new RLike(source, field, new RLikePattern(f.value()));
            };
        }).toList();

        return Predicates.combineOr(expr);
    }
}
