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
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext._LE;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.bind;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.filter;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.find;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.finite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.mapFinite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.mapOpen;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.open;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.select;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.sub;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

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

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(PromqlCommand cmd, AnalyzerContext context) {
        // The command exposes every label of the result series: the full open label space.
        Translation translation = new Translation(cmd, context, null, open(), null);
        return translation.translateFinal();
    }

    /**
     * One translation pass: the command (or an operand fork of it), the analyzer context, and the state of the translateIntermediate
     * being compiled. Independent parts compile separately - like modules.
     */
    private record Translation(
        PromqlCommand cmd,
        AnalyzerContext analyzer,
        /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
        Alias stepBucketAlias,
        /* The columns the result subtree MUST expose. */
        Header parentHeader,
        /* The current translateIntermediate evaluation time (default: @timestamp). */
        Expression time
    ) {

        Configuration configuration() {
            return analyzer.configuration();
        }

        Attribute stepAttr() {
            return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
        }

        /** Translates one merge branch with its own step bucket and evaluation time. */
        IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
            Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
            Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
            var run = new Translation(cmd, analyzer, step, parentHeader, branchTime);
            return run.translateIntermediate(branch, valueId);
        }

        LogicalPlan translateFinal() {
            if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
                VectorMatch match = op.match();
                if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                    // Explicit matching translates to a join whose operands the verifier requires to have concrete label
                    // sets, so its result names every label and carries no packed identity to expose. (A default match
                    // like `a / b` has an open identity and takes the single-branch path below.)
                    // TODO: relax this requirement once expression like `foo on(a, b) / bar` is supprted
                    return doTranslateFinal(doTranslateBinOpInnerJoin(op).plan(), null, false);
                }
            }

            // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
            // cannot compose as a single-value sub-expression.
            // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
            var branches = new ArrayList<LogicalPlan>();
            flattenUnion(cmd.promqlPlan(), branches);

            if (branches.size() == 1) {
                IntermediateResult ir = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
                return doTranslateFinal(ir.plan(), identityColumn(ir.header()), ir.kind().constant);
            }
            // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
            var intermediateResultPlan = doTranslateUnion(
                branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
            );
            return doTranslateFinal(intermediateResultPlan, null, false);
        }

        // -- helpers --

        /**
         * Shared by every `final` translation root. {@code identity} is the column carrying the result's series identity
         * (see {@link #identityColumn}), or null when the result is finite or the union already exposes it as
         * {@code _timeseries}.
         */
        private LogicalPlan doTranslateFinal(LogicalPlan plan, Attribute identity, boolean localRelation) {
            plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan, identity), cmd.valueAttribute());
            return localRelation ? plan : emitByStepFilter(plan);
        }

        /**
         * Union combinator over independently translated tabular results.
         * {@link UnionAll} aligns columns by name and null-fills missing header, then
         * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
         */
        private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
            // Already validated against MergePlan.MAX_BRANCHES by PromqlCommand.verify
            assert MergePlan.exceedsMaxBranches(intermediateResults.size()) == false
                : "invariant: merge branch count ["
                    + intermediateResults.size()
                    + "] must be less of equal MergePlan.MAX_BRANCHES ["
                    + MergePlan.MAX_BRANCHES
                    + "]";

            var source = cmd.source();
            var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
            for (int i = 0; i < intermediateResults.size(); i++) {
                var ir = intermediateResults.get(i);
                LogicalPlan branchPlan = ir.plan();
                // Each branch is projected to its public shape: value, step, its labels, its series identity as
                // `_timeseries` and the branch tag. The union aligns columns by name and the dedup below keys on every
                // non-value column, so this is the exact set the branches must agree on; the collapse's own identity
                // column (e.g. `_timeseries$__name__`) stays behind. The explicit projection also pins the page layout
                // to the branch output: pages cross an exchange and an Eval below (the value double-cast) can
                // name-shadow a column, leaving its channel in the page but not in output() (see #158164).
                var branchOutput = new ArrayList<Attribute>();
                branchOutput.add(ir.valueColumn());
                branchOutput.add(ir.step());
                for (String label : ir.header().finiteColumns()) {
                    branchOutput.add(ir.getExpr(label));
                }
                Attribute identity = identityColumn(ir.header());
                if (identity != null) {
                    if (identity.name().equals(MetadataAttribute.TIMESERIES) == false) {
                        var alias = new Alias(source, MetadataAttribute.TIMESERIES, identity, new NameId());
                        branchPlan = new Eval(source, branchPlan, List.of(alias));
                        identity = alias.toAttribute();
                    }
                    branchOutput.add(identity);
                }
                // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
                branchPlan = emitNullsFilter(source, branchPlan, ir.valueColumn());
                var branchTag = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
                branchOutput.add(branchTag.toAttribute());
                branchPlans.add(new Project(source, new Eval(source, branchPlan, List.of(branchTag)), branchOutput));
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
            var value = ir.value();
            // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
            // source-time filter would push one operand's @timestamp across both sources - skip over InnerJoin.
            Expression timeFilter = plan.anyMatch(p -> p instanceof InnerJoin) ? null : emitBySrcTimeFilter(branch);
            var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
            if (filter != null) {
                plan = pushDownSrcTimestampFilter(plan, filter);
            }

            if (ir.kind().constant == false) {
                // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
                // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
                if (ir.kind().afterInitialAggregation == false) {
                    ir = aggregate(ir.with(plan, value), value);
                    plan = ir.plan();
                    value = ir.value();
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
            if (ir.kind().constant == false) {
                plan = pushDownEvaluationTimestampFilter(plan, branch);
            }

            Kind kind = ir.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
            return new IntermediateResult(plan, ir.header(), valueAlias.toAttribute(), ir.step(), null, kind);
        }

        /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
        private IntermediateResult doTranslateTryInline(IntermediateResult result) {
            Attribute stepAttr = cmd.stepAttribute();
            if (result.kind().constant
                || result.header().isEmpty() == false
                || cmd.start().value() == null
                || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
                return result;
            }
            var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
            var step = plan.output().getFirst();
            var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
            return new IntermediateResult(plan, value, step, result.pendingFilter(), Kind.CONSTANT);
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
        private IntermediateResult doTranslateAddValueEval(IntermediateResult t, Expression value) {
            if (t.kind().afterInitialAggregation == false) {
                return t.with(t.plan(), value);
            }
            Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
            return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute());
        }

        /**
         * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}. The header transposed below the
         * aggregate names every column the subtree must expose, so the child translates once and the aggregate's own
         * columns are read off the returned header. Only {@code AcrossSeriesAggregate} creates plan-level aggregation
         * nodes; within-series aggregates and function calls lower to expressions.
         */
        private IntermediateResult doTranslateAcrossSeriesAgg(AcrossSeriesAggregate agg) {
            var partitionKey = mapFinite(agg.groupings());
            // IN: header describes a layout this node wants from a child subtree
            Header in = switch (agg.grouping()) {
                // by(a,b,c): keep exactly {a,b,c}; rest is null-filled
                case BY -> finite(partitionKey);
                // without(a,b,c): declare the dropped set and widen every pending packed column by it
                case WITHOUT -> union(sub(parentHeader, finite(partitionKey)), open(finite(partitionKey)));
                // this node doesn't have any requirement
                case NONE -> Header.PassThrough;
            };

            Translation translation = new Translation(cmd, analyzer, stepBucketAlias, in, time);
            IntermediateResult ir = translation.doTranslateNode(agg.child());
            if (ir.kind().constant) {
                return ir;
            }

            // OUT: header describes a layout this node produces
            Header out = switch (agg.grouping()) {
                // BY(a,b,c): the result's label set is exactly its keys; whatever the parent needs beyond them is absent
                // on the result and null-fills in the parent's regroup, so `parentHeader` is not part of the output.
                case BY -> finite(mapFinite(agg.output()));
                // The child columns selected by dropping the labels, as the aggregate's own grouping.
                case WITHOUT -> regroupWithout(ir, finite(partitionKey));
                case NONE -> Header.PassThrough;
            };

            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, ir.step(), configuration());
            Expression function = agg.buildEsqlFunction(ir.value(), promqlCtx);
            // A raw operand collapses once, with the operator's function fused into the per-series aggregate.
            return aggregate(ir, out, function, agg.grouping() == WITHOUT);
        }

        /**
         * The columns of a table selected by a {@code without}-like node dropping {@code dropped}, as the grouping of the
         * regroup that follows. Under a packed column the labels are derived columns, so only those the enclosing
         * translation asks for are carried; a finite table keeps every remaining label because they are its label set.
         */
        private Header regroupWithout(IntermediateResult table, Header dropped) {
            Header in = select(table.header(), dropped);
            assert table.header().isOpen() == false || in.isOpen()
                : "invariant: parentHeader ["
                    + parentHeader
                    + "] must declare a packed column excluding "
                    + dropped
                    + ", got "
                    + table.header();
            return in.isOpen() ? filter(in, parentHeader) : in;
        }

        /**
         * Translates an {@link AcrossSeriesReduction} ({@code topk}/{@code bottomk}): collapse the child to one row
         * per series, then rank and keep the top {@code k}. A {@code by} clause only partitions the ranking; it does
         * not change output header.
         */
        private IntermediateResult doTranslateAcrossSeriesReduction(AcrossSeriesReduction plan) {
            if (plan.grouping() == WITHOUT) {
                // TODO: support function like: topk without(...)
                throw new VerificationException("function [{}] is not yet supported with [{}]", plan.functionName(), WITHOUT.name());
            }

            // Ranking happens per series, so the child stays at series grain whatever the enclosing translation regroups
            // by; the partitionKey labels must be exposed to rank within them.
            var partitionKey = mapFinite(plan.groupings());

            // IN: header describes a layout this node wants from a child subtree
            Header in = union(parentHeader, open(), finite(partitionKey));
            IntermediateResult childResult = new Translation(cmd, analyzer, stepBucketAlias, in, time).doTranslateNode(plan.child());
            if (childResult.kind().constant) {
                return childResult;
            }

            // OUT: header describes a layout this node produces;
            // partition labels are grouped even where the child lacks them (null-filled), so the ranking sees them
            Header out = union(childResult.header(), finite(partitionKey));

            var promqlCtx = new PromqlContext(time, AggregateFunction.NO_WINDOW, childResult.step(), configuration());
            IntermediateResult aggregated = aggregate(childResult, out, childResult.value());
            LogicalPlan result = emitTopNBy(plan, aggregated, partitionKey, promqlCtx);

            return aggregated.with(result, aggregated.value());
        }

        /** Ranks the already-collapsed per-series rows and keeps the top {@code k} within each step and partition. */
        private LogicalPlan emitTopNBy(
            AcrossSeriesReduction reduction,
            IntermediateResult table,
            List<String> partitions,
            PromqlContext promqlContext
        ) {
            var groupings = new ArrayList<Expression>();
            groupings.add(table.step());
            LogicalPlan plan = table.plan();
            if (reduction.grouping() == AcrossSeriesAggregate.Grouping.BY) {
                for (String partition : partitions) {
                    Attribute partitionExpr = table.getExpr(partition);
                    assert partitionExpr != null : "invariant: ranking partition " + partition + " must be produced by the child";
                    groupings.add(partitionExpr);
                }
            }
            var order = (Order) reduction.buildEsqlFunction(table.value(), promqlContext);
            return new TopNBy(
                reduction.source(),
                plan,
                order != null ? List.of(order) : List.of(),
                new ToInteger(reduction.source(), reduction.parameters().getFirst()),
                groupings
            );
        }

        /**
         * The single aggregation primitive: a raw table collapses once through the innermost
         * {@link TimeSeriesAggregate}; an already-collapsed table regroups through {@link Aggregate}.
         * A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
         * dimensions from splitting rows and double-counting, then unpacks them afterwards.
         * {@code grouping} names the output columns; it is bound against the input's columns, null-filling labels the
         * input lacks. Without one, the aggregate runs over the table's own grain: every carried column stays a key.
         */
        private IntermediateResult aggregate(IntermediateResult input, Expression function) {
            return aggregate(input, input.header(), function, false);
        }

        private IntermediateResult aggregate(IntermediateResult input, Header grouping, Expression function) {
            return aggregate(input, grouping, function, false);
        }

        private IntermediateResult aggregate(IntermediateResult input, Header grouping, Expression function, boolean packed) {
            Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
            if (input.kind().afterInitialAggregation == false) {
                assert input.kind() == Kind.BEFORE_INITIAL_AGGREGATE : "invariant: aggregates take raw or collapsed tables";
                return emitCollapse(input, grouping, value);
            }
            return emitRegroup(input, grouping, value, grouping.isOpen() || packed);
        }

        private static IntermediateResult table(LogicalPlan plan, IntermediateResult input, Alias value, Header header) {
            return new IntermediateResult(
                plan,
                header,
                value.toAttribute(),
                input.step(),
                input.pendingFilter(),
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }

        /**
         * The innermost aggregate groups on the child's already-defined columns. Packed groupings receive distinct
         * output names without exposing their source representation to the aggregate.
         */
        private IntermediateResult emitCollapse(IntermediateResult input, Header grouping, Alias value) {
            Source source = cmd.promqlPlan().source();
            LogicalPlan plan = input.plan();
            boolean groupsBySeries = grouping.isOpen() || grouping.finiteColumns().isEmpty() == false;
            Expression agg = value.child();
            // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
            // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
            // AggregateFunction inside the Aggregate node:
            // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
            // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
            // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
            // Guarded by groupsBySeries because without any series grouping (e.g. constants like vector(5))
            // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
            boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
            if (groupsBySeries && wrapWithValues) {
                value = value.replaceChild(new Values(agg.source(), agg));
            }

            // Packed columns are grouped by increasing exclusions, followed by the requested explicit labels.
            // Every column is functionally dependent on the first packed column, so grouping by all of them
            // preserves per-series granularity while making the full header available to the surrounding query.
            // A packing is aliased to its derived name so the aggregate output does not expose its source representation;
            // the alias is a grouping expression the aggregate itself defines, so the null-fills are taken before it.
            Header bound = bind(grouping, input.header());
            List<Alias> nulls = bound.nullFills(plan);
            if (nulls.isEmpty() == false) {
                plan = new Eval(source, plan, nulls);
            }
            var groupKeys = new ArrayList<NamedExpression>();
            var columnExpr = new LinkedHashMap<>(bound.columnExpr());
            for (var col : bound.openColumns()) {
                Attribute expr = bound.getExpr(col);
                Alias packed = new Alias(source, mapOpen(col), expr, expr.id());
                groupKeys.add(packed);
                columnExpr.put(mapOpen(col), packed.toAttribute());
            }
            bound.finiteColumns().forEach(name -> groupKeys.add(bound.getExpr(name)));
            Header out = new Header(bound.finiteColumns(), bound.openColumns(), columnExpr);
            plan = new TimeSeriesAggregate(
                source,
                plan,
                groupings(stepBucketAlias, groupKeys),
                aggregates(value, input.step(), out.expressions()),
                null,
                time,
                TimeSeriesAggregate.Origin.PROMQL_COMMAND
            );
            return table(plan, input, value, out);
        }

        /**
         * Regroups an already-aggregated table. Every regroup consumes its child's columns and null-fills missing
         * grouping columns. A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
         * dimensions from splitting rows and double-counting, then unpacks them afterwards.
         */
        private IntermediateResult emitRegroup(IntermediateResult input, Header grouping, Alias value, boolean requiresPacking) {
            Source source = cmd.source();
            Attribute step = input.step();
            LogicalPlan plan = input.plan();
            if (value.child() instanceof AggregateFunction == false) {
                value = value.replaceChild(new Values(value.child().source(), value.child()));
            }
            // a declared label the child lacks is absent from every series: grouped under null, like Prometheus
            Header out = bind(grouping, input.header());
            List<Alias> nulls = out.nullFills(plan);
            if (nulls.isEmpty() == false) {
                plan = new Eval(source, plan, nulls);
            }
            List<Attribute> keys = out.expressions();

            // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
            if (requiresPacking == false || keys.isEmpty()) {
                plan = new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
                return table(plan, input, value, out);
            }
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
            UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
            List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
            projections.addAll(unpackedDims);
            Map<NameId, Attribute> unpacked = new HashMap<>();
            unpackedDims.forEach(attribute -> unpacked.put(attribute.id(), attribute));
            Header rebound = out.map(expr -> unpacked.get(expr.id()));
            return table(new Project(source, unpackDims, projections), input, value, rebound);
        }

        private IntermediateResult doTranslateHistogramFunction(HistogramFunctionCall function) {
            // Classic histogram functions collapse the `le` bucket dimension like a `without (le)` would, and read the
            // bucket bound off the `le` column itself, so the child must also expose it by name.
            Header in = union(sub(parentHeader, _LE), open(_LE), _LE);
            IntermediateResult result = new Translation(cmd, analyzer, stepBucketAlias, in, time).doTranslateNode(function.child());
            if (result.kind().constant) {
                return result;
            }

            // native histograms - distinguishable only at this point in planning are regular value transformations.
            if (result.value().resolved() && result.value().dataType().isHistogram()) {
                return doTranslateFunc(
                    new ValueTransformationFunction(function.source(), function.child(), function.definition(), function.parameters())
                );
            }

            // Classic counter-backed histograms need the special treatment below.
            var le = result.getExpr(HistogramFunctionCall.LE_LABEL);
            if (le == null) {
                // like prometheus, return warning and drop series w/o `le`
                HeaderWarning.addWarning(function.functionName() + ": input vector has no le label; no buckets to evaluate");
                var skipAllFilter = new Filter(function.source(), result.plan(), Literal.FALSE);
                var nullGrouping = new Values(function.source(), new Literal(function.source(), null, DataType.DOUBLE));
                return aggregate(result.with(skipAllFilter, result.value()), nullGrouping);
            }

            if (result.kind().afterInitialAggregation == false) {
                result = aggregate(result, result.value());
                le = result.getExpr(HistogramFunctionCall.LE_LABEL);
                assert le != null : "invariant: [ " + HistogramFunctionCall.LE_LABEL + " ] parentHeader";
            }

            // Bucket counts are consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
            // The `without (le)`-like regroup exposes the child columns selected by dropping the labels.
            Header out = regroupWithout(result, _LE);
            Expression count = new ToDouble(function.source(), result.value());

            return aggregate(result, out, function.buildAggregateFunction(count, le), true);
        }

        /** scalar(): collapse to one value per step, e.g. scalar(sum by (cluster) (metric)). */
        private IntermediateResult doTranslateScalarConvertion(ScalarConversionFunction scalarFunc) {
            // The result has no labels, so the child's label set is irrelevant: it exposes none.
            IntermediateResult child = new Translation(cmd, analyzer, stepBucketAlias, Header.PassThrough, time).doTranslateNode(
                scalarFunc.child()
            );
            if (child.value().foldable()) {
                Expression value = new ToDouble(scalarFunc.source(), child.value());
                return new IntermediateResult(child.plan(), value, child.step(), child.pendingFilter());
            }
            var scalarExpr = new Scalar(scalarFunc.source(), child.value());
            return aggregate(child, Header.PassThrough, scalarExpr);
        }

        /** Translates a generic PromQL function call (rate, ceil, abs, etc.) into an expression over the child's value. */
        private IntermediateResult doTranslateFunc(PromqlFunctionCall functionCall) {
            IntermediateResult child = doTranslateNode(functionCall.child());
            if (child.kind().constant) {
                return child;
            }
            Expression window = AggregateFunction.NO_WINDOW;
            if (functionCall.child() instanceof RangeSelector rangeSelector) {
                window = isImplicitRangePlaceholder(rangeSelector.range()) ? cmd.resolveImplicitRangeWindow() : rangeSelector.range();
            }
            var promqlCtx = new PromqlContext(time, window, child.step(), configuration());
            return doTranslateAddValueEval(child, functionCall.buildEsqlFunction(child.value(), promqlCtx));
        }

        /**
         * Translates a {@code label_replace}/{@code label_join} into a derived label column.
         * <p>
         * The child is first collapsed to one row per series (forcing the initial aggregate when it has not happened yet), so
         * the source labels are materialized as columns to derive from. The destination value is then computed with an
         * {@link Eval} under the destination's stable id and declared as a label of the result, so the enclosing
         * {@code by(...)} aggregation groups on it exactly as it would on a stored label. The derived label shadows a stored
         * label of the same name: the old column is projected away and its binding is replaced with the derived one.
         * <p>
         * Because ES|QL treats {@code null} and {@code ""} as distinct grouping keys while Prometheus treats an absent label
         * and an empty label value alike, every "label absent" outcome is normalized to {@code ""}: an absent source is
         * coalesced to {@code ""} before matching, and {@code label_replace}'s no-match ({@code null}) is coalesced back to
         * {@code ""}. All such series therefore fall into the same group, matching Prometheus.
         */
        private IntermediateResult doTranslateMetadataManipulation(MetadataManipulationFunction relabel) {
            // IN:
            // The child must expose the labels the derivation reads, on top of whatever the enclosing translation requires.
            Header in = union(parentHeader, finite(relabel.sourceLabels()));
            IntermediateResult child = new Translation(cmd, analyzer, stepBucketAlias, in, time).doTranslateNode(relabel.child());
            if (child.kind().constant) {
                return child;
            }

            // Collapse to one row per series so the source labels exist as columns; this mirrors the seam in
            // translateIntermediate that forces the initial per-series aggregate for a not-yet-aggregated subtree.
            IntermediateResult aggregated = child.kind().afterInitialAggregation ? child : aggregate(child, child.value());

            Source source = relabel.source();
            Attribute destination = relabel.destination();
            Expression destinationValue = relabel.definition() == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
                ? labelReplaceValue(source, relabel, aggregated)
                : labelJoinValue(source, relabel, aggregated);

            String name = mapFinite(destination);
            Alias derived = new Alias(source, destination.name(), destinationValue, destination.id());
            LogicalPlan plan = new Eval(cmd.source(), aggregated.plan(), List.of(derived));
            Attribute previous = aggregated.getExpr(name);
            if (previous != null && previous.id().equals(derived.id()) == false && plan.outputSet().contains(previous)) {
                plan = new Project(
                    cmd.source(),
                    plan,
                    plan.output().stream().filter(attribute -> attribute.id().equals(previous.id()) == false).toList()
                );
            }

            // OUT:
            Header out = bind(aggregated.header(), name, derived.toAttribute());

            return aggregated.with(plan, out, aggregated.value());
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
        private Expression labelReplaceValue(Source source, MetadataManipulationFunction relabel, IntermediateResult table) {
            List<Expression> params = relabel.parameters();
            String srcLabel = literalString(params.get(2));
            Expression regex = params.get(3);
            Expression replacement = params.get(1);
            Expression src = sourceLabelValue(source, table, srcLabel);
            Expression extracted = new RegexExpand(source, src, regex, replacement);
            Expression existingDst = sourceLabelValue(source, table, mapFinite(relabel.destination()));
            return new Coalesce(source, extracted, List.of(existingDst));
        }

        /**
         * The {@code label_join} destination value: the source label values coalesced to {@code ""} and joined by the
         * separator. With no source labels the result is {@code ""} - the same "absent" grouping key produced by
         * {@code label_replace}; a single source label is copied verbatim (no separator). With two or more source labels the
         * separator is inserted between every value, so even all-empty sources yield the separator run (for example a
         * {@code "-"} separator over two absent labels produces {@code "-"}), matching Prometheus.
         */
        private Expression labelJoinValue(Source source, MetadataManipulationFunction relabel, IntermediateResult table) {
            List<Expression> params = relabel.parameters();
            Literal separator = Literal.keyword(source, literalString(params.get(1)));

            List<Expression> parts = new ArrayList<>(2 * params.size() + 1);
            for (int i = 2; i < params.size(); i++) {
                if (parts.isEmpty() == false) {
                    parts.add(separator);
                }
                parts.add(sourceLabelValue(source, table, literalString(params.get(i))));
            }

            return switch (parts.size()) {
                case 0 -> Literal.keyword(source, "");
                case 1 -> parts.getFirst();
                default -> new Concat(source, parts.getFirst(), parts.subList(1, parts.size()));
            };
        }

        /**
         * The value of a source label as a non-null string: {@code COALESCE(ToString(label), "")}, or {@code ""} if the
         * table does not carry the label. The lookup reads the table's plan, so it sees stored labels only: a destination an
         * enclosing {@code by(dst)} requires is a name in the header, never a column here, and cannot resolve to itself.
         */
        private Expression sourceLabelValue(Source source, IntermediateResult table, String labelName) {
            Attribute label = table.getExpr(labelName);
            if (label == null) {
                return Literal.keyword(source, "");
            }
            Expression stringValue = DataType.isString(label.dataType()) ? label : new ToString(source, label, configuration());
            return new Coalesce(source, stringValue, List.of(Literal.keyword(source, "")));
        }

        /** Translates a scalar function (time(), etc.): an expression over the unchanged source. */
        private IntermediateResult doTranslateScalarFunc(ScalarFunction scalarFunction) {
            var function = scalarFunction.buildEsqlFunction(new PromqlContext(cmd.timestamp(), null, cmd.stepAttribute(), configuration()));
            return new IntermediateResult(cmd.child(), function, stepAttr());
        }

        /** Translates explicit vector matching as a join; other binary operators compose over a shared frame. */
        private IntermediateResult doTranslateBinaryOp(VectorBinaryOperator op) {
            if (op.match().filter() == VectorMatch.Filter.NONE && op.match().grouping() == Joining.NONE) {
                boolean scalarOperand = op.left().resolved() && getType(op.left()) == SCALAR
                    || op.right().resolved() && getType(op.right()) == SCALAR;
                boolean nestedMatch = anyMatchVectorBinaryOperator(op.left()) || anyMatchVectorBinaryOperator(op.right());
                // Operands over one label set fold into a shared aggregate. Different concrete label sets match like
                // Prometheus does, pair by pair on the actual labels, which only the join expresses.
                if (scalarOperand || (nestedMatch == false && op.hasMismatchedLabelSets() == false)) {
                    return doTranslateBinaryOpAggregate(op);
                }
            }
            return doTranslateBinOpInnerJoin(op);
        }

        /** Composes a binary operator as an expression over the operands' shared aggregate. */
        private IntermediateResult doTranslateBinaryOpAggregate(VectorBinaryOperator binaryOp) {
            Translation childTranslation = new Translation(cmd, analyzer, stepBucketAlias, parentHeader, time);
            IntermediateResult left = childTranslation.doTranslateNode(binaryOp.left());
            Expression leftExpr = new ToDouble(left.value().source(), left.value());
            if (binaryOp instanceof VectorBinaryComparison comp && comp.filterMode()) {
                return left.with(left.plan(), leftExpr);
            }
            IntermediateResult right = childTranslation.doTranslateNode(binaryOp.right());
            Expression rightExpr = new ToDouble(right.value().source(), right.value());
            Expression binaryExpr = binaryOp.binaryOp().asFunction().create(binaryOp.source(), leftExpr, rightExpr, configuration());

            LogicalPlan plan;
            Expression filter;
            IntermediateResult ir;
            if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
                plan = emitBinaryOperatorAggregateExpression(left, right);
                ir = left;
                filter = null;
            } else {
                ir = getType(binaryOp.left()) != SCALAR ? left
                    : getType(binaryOp.right()) != SCALAR ? right
                    : left.kind().afterInitialAggregation ? left
                    : right;
                plan = ir.plan();
                filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
            }
            Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
                ? Kind.AFTER_INITIAL_AGGREGATE
                : Kind.BEFORE_INITIAL_AGGREGATE;

            IntermediateResult result = new IntermediateResult(plan, ir.header(), null, ir.step(), filter, kind);
            return doTranslateAddValueEval(result, binaryExpr);
        }

        /**
         * Translates a vector-matched join operator into an {@link InnerJoin}: each operand becomes an independent series
         * pipeline, joined on shared {@code step} + label keys, and the result value is computed on the joined rows.
         * The operands compile against the labels the join requires, like any other header push-down: a parentHeader label
         * comes back as a concrete column wherever the operand can carry it, and a label the operand dropped stays
         * absent and null-fills at the join.
         */
        private IntermediateResult doTranslateBinOpInnerJoin(VectorBinaryOperator op) {
            // TODO: revisit once expression like foo on(a,b) / bar is supported
            // Verifier admits only operands with concrete label sets here, so the join result names every label: the
            // operator's declared output plus whatever the enclosing translation asks for by name (null-filled when the
            // match dropped it). Packed columns stop at the join for that reason, not because joins are finite in general.
            Header in = union(finite(mapFinite(op.output())), finite(parentHeader.finiteColumns()));
            VectorMatch match = op.match();
            if (match.filter() == VectorMatch.Filter.ON) {
                in = union(union(finite(mapFinite(op.output())), finite(parentHeader.finiteColumns())), finite(match.filterLabels()));
            } else if (match.filter() == VectorMatch.Filter.IGNORING) {
                // The key is each operand's own label set minus the ignored labels: a packed column for an opaque operand.
                in = union(union(finite(mapFinite(op.output())), finite(parentHeader.finiteColumns())), open(match.filterLabels()));
            } else {
                // No on/ignoring: the key is each operand's whole label set. The verifier admits only operands with
                // concrete label sets here, so the operator's declared output already names every label of both sides
                // and the header needs no widening.
                assert match.filter() == VectorMatch.Filter.NONE : "unexpected vector match filter " + match.filter();
                assert hasConcreteLabels(op.left()) && hasConcreteLabels(op.right())
                    : "invariant: an unmatched join needs operands with concrete label sets [" + op.sourceText() + "]";
            }
            Translation childTranslation = new Translation(cmd, analyzer, stepBucketAlias, in, time);
            return new VectorBinaryOperatorLayout(op).command(cmd)
                .configuration(configuration())
                .stepId(stepAttr().id())
                .header(union(finite(mapFinite(op.output())), finite(parentHeader.finiteColumns())))
                .left(childTranslation.translateIntermediate(op.left(), new NameId(), new NameId()))
                .right(childTranslation.translateIntermediate(op.right(), new NameId(), new NameId()))
                .result();
        }

        /** Fold left and right aggregates into a single plan. */
        private LogicalPlan emitBinaryOperatorAggregateExpression(IntermediateResult left, IntermediateResult right) {
            var names = new TemporaryNameGenerator.Monotonic();
            var rightAgg = right.plan().collect(Aggregate.class).getFirst();
            List<Expression> rightGroupings = rightAgg.groupings();

            var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
                List<Expression> leftGroupings = leftAgg.groupings();
                Set<String> leftGroupingNames = new HashSet<>();
                for (Expression grouping : leftGroupings) {
                    if (grouping instanceof NamedExpression ne) {
                        leftGroupingNames.add(ne.name());
                    }
                }
                Set<String> rightGroupingNames = new HashSet<>();
                for (Expression grouping : rightGroupings) {
                    if (grouping instanceof NamedExpression ne) {
                        rightGroupingNames.add(ne.name());
                    }
                }
                boolean groupingsCompatible = leftGroupings.size() == rightGroupings.size() && leftGroupingNames.equals(rightGroupingNames);

                if (groupingsCompatible == false) {
                    throw new VerificationException(
                        "binary operations between vectors with mismatched grouping keys are not yet supported"
                    );
                }

                // Grouping columns match by name. Relation fields share one attribute on both sides, but a packing alias
                // (`_timeseries$__name__`) is minted per collapse: rebind the right side's keys to the left side's so the
                // fused aggregate carries each key once and every reference resolves against the surviving grouping.
                Map<String, Attribute> leftKeys = new HashMap<>();
                for (Expression grouping : leftAgg.groupings()) {
                    if (grouping instanceof NamedExpression ne) {
                        leftKeys.put(ne.name(), ne.toAttribute());
                    }
                }
                Map<NameId, Attribute> rightToLeft = new HashMap<>();
                for (Expression grouping : rightAgg.groupings()) {
                    if (grouping instanceof NamedExpression ne && leftKeys.containsKey(ne.name())) {
                        rightToLeft.put(ne.toAttribute().id(), leftKeys.get(ne.name()));
                    }
                }
                List<? extends Expression> rightAggregates = rightAgg.aggregates()
                    .stream()
                    .map(e -> e.transformUp(Attribute.class, a -> rightToLeft.getOrDefault(a.id(), a)))
                    .toList();

                var uniqueAggregates = new LinkedHashSet<Expression>();
                uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
                uniqueAggregates.addAll(withFilter(rightAggregates, right.pendingFilter()));

                // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
                // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
                // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
                var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                    if (e instanceof Alias a) {
                        return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
                    }
                    return e;
                }).toList();

                return leftAgg.with(leftAgg.child(), leftGroupings, newAggregates);
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
            Expression matcherExpression = emitMatchersPredicateExpression(
                selector.source(),
                selector.labels(),
                selector.labelMatchers(),
                configuration()
            );

            if (selector instanceof LiteralSelector literalSelector) {
                Expression literal = literalSelector.literal();
                if (foldedPlan != null) {
                    // a compile-time relation carries its own step column
                    Attribute foldedStep = find(foldedPlan.output(), cmd.stepColumnName());
                    return new IntermediateResult(foldedPlan, literal, foldedStep, matcherExpression, Kind.CONSTANT);
                }
                return new IntermediateResult(input, literal, stepAttr(), matcherExpression);
            }
            if (foldedPlan != null) {
                var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
                return new IntermediateResult(empty, Literal.NULL, cmd.stepAttribute(), null, Kind.CONSTANT);
            }

            // An instant selector maps to LastOverTime to get the latest sample per time series.
            Expression expr = selector instanceof InstantSelector
                ? new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, time)
                : selector.series();

            // output contains all runtime mapped fields, select only fields we need
            var relationFields = input.output()
                .stream()
                .filter(
                    dim -> (dim instanceof FieldAttribute fa && fa.isDimension()) && (dim instanceof TimeSeriesMetadataAttribute == false)
                )
                .toList();

            // IN: only `parentHeader` labels that exist on the relation;
            // consumers null-fill any absent parentHeader label.
            Header in = filter(parentHeader, finite(mapFinite(relationFields)));

            // OUT: the requirement bound column by column to the relation;
            // a packed column is produced by a `TimeSeriesMetadataAttribute`, added to the relation if absent
            Header out = in;

            // packed columns
            var additional = new ArrayList<Attribute>();
            for (var col : in.openColumns()) {
                Attribute attr;
                if ((attr = find(input.output(), col)) == null) {
                    attr = new TimeSeriesMetadataAttribute(selector.source(), col);
                    additional.add(attr);
                }
                out = bind(out, col, attr);
            }

            // regular columns
            for (var col : in.finiteColumns()) {
                out = bind(out, col, find(relationFields, col));
            }

            if (additional.isEmpty() == false) {
                input = input.transformUp(EsRelation.class, relation -> relation.withAdditionalAttributes(additional));
            }

            return new IntermediateResult(input, out, expr, stepAttr(), matcherExpression, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
        private LogicalPlan emitFinalProjection(LogicalPlan plan, Attribute identity) {
            var lookupMap = new HashMap<String, Attribute>();
            for (var attr : plan.output()) {
                lookupMap.put(attr.name(), attr);
            }
            // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
            // the label alone, so fall back to the canonical name.
            for (var attr : plan.output()) {
                lookupMap.putIfAbsent(mapFinite(attr), attr);
            }
            // Packed columns travel under names derived from their exclusions,
            // e.g.: * \ {a,b} is encoded as `_timeseries$a$b`;
            // Final output drops `$a$b` suffix.
            if (identity != null) {
                lookupMap.put(MetadataAttribute.TIMESERIES, identity);
            }
            var projected = new ArrayList<Attribute>();
            var evals = new ArrayList<Alias>();
            for (var attr : cmd.output()) {
                var lookupAttr = lookupMap.get(attr.name());
                if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                    var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                    evals.add(alias);
                    projected.add(alias.toAttribute());
                } else {
                    projected.add(attr);
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
                return addEvaluationTimestamp(plan, time);
            }
            return plan;
        }

        /** Joined operands already own their evaluation times; only visit this branch's source. */
        private LogicalPlan addEvaluationTimestamp(LogicalPlan plan, Alias timestamp) {
            if (plan instanceof InnerJoin) {
                return plan;
            }
            if (plan instanceof EsRelation) {
                return new Eval(cmd.source(), plan, List.of(timestamp));
            }
            return plan.replaceChildren(plan.children().stream().map(child -> addEvaluationTimestamp(child, timestamp)).toList());
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

    /**
     * The column carrying a table's series identity: its first packed column, the one with the fewest exclusions. Null
     * for a finite table. Named `_timeseries` when it excludes nothing, `_timeseries$…` otherwise.
     */
    private static Attribute identityColumn(Header header) {
        return header.isOpen() ? header.getExpr(header.openColumns().iterator().next()) : null;
    }

    // -- pure helpers, independent of the running translation --

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

    /** The string value of a keyword-literal PromQL function argument. */
    private static String literalString(Expression literal) {
        return BytesRefs.toString(((Literal) literal).value());
    }

    private static boolean anyMatchVectorBinaryOperator(LogicalPlan plan) {
        return plan.anyMatch(p -> {
            if (p instanceof VectorBinaryOperator vbo) {
                VectorMatch match = vbo.match();
                return match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE;
            }
            return false;
        });
    }

    /**
     * Whether the operand's declared output names every label it carries, i.e. it exposes no packed {@code _timeseries}
     * column. Mirrors the verifier's admission rule for join-composed operators without on/ignoring.
     */
    private static boolean hasConcreteLabels(LogicalPlan operand) {
        return operand.output().stream().noneMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
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
