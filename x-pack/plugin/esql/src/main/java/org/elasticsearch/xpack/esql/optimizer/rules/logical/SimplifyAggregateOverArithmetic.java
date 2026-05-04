/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.ADD;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MUL;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.SUB;

/**
 * Simplifies aggregations over linear arithmetic expressions by pushing the constant operation
 * out of the aggregate and into a post-aggregation correction.
 * <p>
 * For example:
 * <ul>
 *   <li>{@code SUM(col + K)} becomes {@code SUM(col)} with post-agg: {@code sum_col + K * COUNT(*)}</li>
 *   <li>{@code SUM(col * K)} becomes {@code SUM(col)} with post-agg: {@code sum_col * K}</li>
 *   <li>{@code MIN(col + K)} becomes {@code MIN(col)} with post-agg: {@code min_col + K}</li>
 *   <li>{@code MIN(col * K)} where K &gt; 0 becomes {@code MIN(col)} with post-agg: {@code min_col * K}</li>
 *   <li>{@code MIN(col * K)} where K &lt; 0 becomes {@code MAX(col)} with post-agg: {@code max_col * K}</li>
 * </ul>
 * <p>
 * Safety: Div and Mod are NOT handled (integer truncation changes semantics). Mul by zero is skipped.
 * Sub(col, K) is treated as a subtraction in the correction for SUM, MIN/MAX.
 * <p>
 * This rule must run BEFORE {@link ReplaceAggregateNestedExpressionWithEval} in the substitution batch.
 */
public final class SimplifyAggregateOverArithmetic extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        List<? extends NamedExpression> aggs = aggregate.aggregates();

        // first pass: identify candidates
        boolean hasCandidates = false;
        for (NamedExpression agg : aggs) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (extractSimplification(af) != null) {
                    hasCandidates = true;
                    break;
                }
            }
        }
        if (hasCandidates == false) {
            return aggregate;
        }

        // maps for deduplication of simplified aggregates
        // key: canonical form of the simplified aggregate, value: the alias's attribute
        Map<Expression, Attribute> simplifiedAggToAttr = new HashMap<>();

        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
        List<Alias> postAggEvals = new ArrayList<>();
        List<NamedExpression> projections = new ArrayList<>();
        boolean changed = false;
        int counter = 0;

        for (NamedExpression agg : aggs) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                SimplificationResult result = extractSimplification(af);
                if (result != null) {
                    changed = true;
                    Source source = af.source();

                    // create or reuse the simplified aggregate
                    AggregateFunction simplifiedAgg = result.simplifiedAgg;
                    Expression canonical = simplifiedAgg.canonical();
                    Attribute aggAttr = simplifiedAggToAttr.get(canonical);
                    if (aggAttr == null) {
                        String name = TemporaryNameGenerator.temporaryName(simplifiedAgg, af, counter++);
                        Alias aggAlias = new Alias(source, name, simplifiedAgg, null, true);
                        aggAttr = aggAlias.toAttribute();
                        simplifiedAggToAttr.put(canonical, aggAttr);
                        newAggs.add(aggAlias);
                    }

                    // if we need a COUNT(*) for SUM+Add/Sub correction, create or reuse it
                    Attribute countAttr = null;
                    if (result.needsCount) {
                        Count countStar = new Count(source, Literal.keyword(source, StringUtils.WILDCARD), af.filter(), af.window());
                        Expression countCanonical = countStar.canonical();
                        countAttr = simplifiedAggToAttr.get(countCanonical);
                        if (countAttr == null) {
                            String countName = TemporaryNameGenerator.temporaryName(countStar, af, counter++);
                            Alias countAlias = new Alias(source, countName, countStar, null, true);
                            countAttr = countAlias.toAttribute();
                            simplifiedAggToAttr.put(countCanonical, countAttr);
                            newAggs.add(countAlias);
                        }
                    }

                    Expression correctionExpr = result.buildCorrection(aggAttr, countAttr);

                    // create the post-agg eval alias, preserving the original alias id
                    Alias evalAlias = new Alias(alias.source(), alias.name(), correctionExpr, alias.toAttribute().id());
                    postAggEvals.add(evalAlias);
                    projections.add(evalAlias.toAttribute());
                } else {
                    // not a candidate, pass through
                    newAggs.add(agg);
                    projections.add(agg.toAttribute());
                }
            } else {
                // grouping key or other non-alias
                newAggs.add(agg);
                projections.add(agg.toAttribute());
            }
        }

        if (changed == false) {
            return aggregate;
        }

        Source source = aggregate.source();
        LogicalPlan plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
        if (postAggEvals.isEmpty() == false) {
            plan = new Eval(source, plan, postAggEvals);
        }
        plan = new Project(source, plan, projections);
        return plan;
    }

    /**
     * Extracts the simplification result for an aggregate function, or null if the pattern doesn't match.
     */
    static SimplificationResult extractSimplification(AggregateFunction af) {
        Expression field = af.field();

        // only handle arithmetic operations
        if (field instanceof ArithmeticOperation == false) {
            return null;
        }
        ArithmeticOperation arith = (ArithmeticOperation) field;

        // reject nested aggregates
        Holder<Boolean> hasNestedAgg = new Holder<>(false);
        field.forEachDown(AggregateFunction.class, unused -> hasNestedAgg.set(true));
        if (hasNestedAgg.get()) {
            return null;
        }

        // identify the literal and non-literal sides
        Expression left = arith.left();
        Expression right = arith.right();
        Expression colExpr;
        Literal literal;
        boolean literalOnLeft;

        if (left instanceof Literal lit && right instanceof Literal == false) {
            literal = lit;
            colExpr = right;
            literalOnLeft = true;
        } else if (right instanceof Literal lit && left instanceof Literal == false) {
            literal = lit;
            colExpr = left;
            literalOnLeft = false;
        } else {
            return null;
        }

        // only handle numeric literals
        if (literal.value() instanceof Number == false) {
            return null;
        }

        String opSymbol = arith.symbol();

        // only handle Add, Sub, Mul
        if (opSymbol.equals(ADD.symbol())) {
            return simplifyAddSub(af, arith, colExpr, literal, true, literalOnLeft);
        } else if (opSymbol.equals(SUB.symbol())) {
            // Sub: col - K or K - col
            if (literalOnLeft) {
                // K - col: not a simple linear form we can optimize
                return null;
            }
            return simplifyAddSub(af, arith, colExpr, literal, false, false);
        } else if (opSymbol.equals(MUL.symbol())) {
            return simplifyMul(af, arith, colExpr, literal);
        }
        // Div, Mod: not handled
        return null;
    }

    /**
     * Handles SUM(col +/- K), MIN(col +/- K), MAX(col +/- K).
     */
    private static SimplificationResult simplifyAddSub(
        AggregateFunction af,
        ArithmeticOperation arith,
        Expression colExpr,
        Literal literal,
        boolean isAdd,
        boolean literalOnLeft
    ) {
        if (af instanceof Sum) {
            // SUM(col + K) = SUM(col) + K * COUNT(*)
            // SUM(col - K) = SUM(col) - K * COUNT(*)
            // SUM(K + col) = same as SUM(col + K) (commutative for add)
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new SumAddSubResult(simplifiedAgg, literal, isAdd, arith);
        } else if (af instanceof Min) {
            // MIN(col + K) = MIN(col) + K
            // MIN(col - K) = MIN(col) - K
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new MinMaxAddSubResult(simplifiedAgg, literal, isAdd, arith);
        } else if (af instanceof Max) {
            // MAX(col + K) = MAX(col) + K
            // MAX(col - K) = MAX(col) - K
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new MinMaxAddSubResult(simplifiedAgg, literal, isAdd, arith);
        } else if (af instanceof Avg) {
            // AVG(col + K) = AVG(col) + K
            // AVG(col - K) = AVG(col) - K
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new MinMaxAddSubResult(simplifiedAgg, literal, isAdd, arith);
        }
        return null;
    }

    /**
     * Handles SUM(col * K), MIN(col * K), MAX(col * K).
     */
    private static SimplificationResult simplifyMul(AggregateFunction af, ArithmeticOperation arith, Expression colExpr, Literal literal) {
        double k = ((Number) literal.value()).doubleValue();
        // K == 0: skip (SUM(col * 0) is always 0 but that's a different optimization)
        if (k == 0) {
            return null;
        }

        if (af instanceof Sum) {
            // SUM(col * K) = SUM(col) * K
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new MulResult(simplifiedAgg, literal);
        } else if (af instanceof Min || af instanceof Max) {
            // For K > 0: MIN(col * K) = MIN(col) * K, MAX(col * K) = MAX(col) * K
            // For K < 0: MIN(col * K) = MAX(col) * K, MAX(col * K) = MIN(col) * K (order reversal)
            AggregateFunction simplifiedAgg;
            if (k > 0) {
                simplifiedAgg = af.withField(colExpr);
            } else {
                // K < 0: swap MIN <-> MAX
                if (af instanceof Min min) {
                    simplifiedAgg = new Max(min.source(), colExpr, min.filter(), min.window());
                } else {
                    Max max = (Max) af;
                    simplifiedAgg = new Min(max.source(), colExpr, max.filter(), max.window());
                }
            }
            return new MulResult(simplifiedAgg, literal);
        } else if (af instanceof Avg) {
            // AVG(col * K) = AVG(col) * K
            AggregateFunction simplifiedAgg = af.withField(colExpr);
            return new MulResult(simplifiedAgg, literal);
        }
        return null;
    }

    /**
     * The result of simplifying an aggregate function: the simplified aggregate and how to build the correction.
     */
    abstract static class SimplificationResult {
        final AggregateFunction simplifiedAgg;
        final boolean needsCount;

        SimplificationResult(AggregateFunction simplifiedAgg, boolean needsCount) {
            this.simplifiedAgg = simplifiedAgg;
            this.needsCount = needsCount;
        }

        /**
         * Builds the post-aggregation correction expression.
         * @param aggRef attribute referencing the simplified aggregate result
         * @param countRef attribute referencing COUNT(*), or null if not needed
         */
        abstract Expression buildCorrection(Attribute aggRef, Attribute countRef);
    }

    /**
     * SUM(col + K) -> SUM(col) + K * COUNT(*)
     * SUM(col - K) -> SUM(col) - K * COUNT(*)
     */
    static final class SumAddSubResult extends SimplificationResult {
        private final Literal literal;
        private final boolean isAdd;
        private final ArithmeticOperation originalOp;

        SumAddSubResult(AggregateFunction simplifiedAgg, Literal literal, boolean isAdd, ArithmeticOperation originalOp) {
            super(simplifiedAgg, true);
            this.literal = literal;
            this.isAdd = isAdd;
            this.originalOp = originalOp;
        }

        @Override
        Expression buildCorrection(Attribute aggRef, Attribute countRef) {
            Source src = originalOp.source();
            // K * COUNT(*)
            Expression kTimesCount = new Mul(src, literal, countRef);
            // SUM(col) +/- K * COUNT(*)
            Configuration config = extractConfiguration(originalOp);
            if (isAdd) {
                return new Add(src, aggRef, kTimesCount, config);
            } else {
                return new Sub(src, aggRef, kTimesCount, config);
            }
        }
    }

    /**
     * MIN/MAX(col + K) -> MIN/MAX(col) + K
     * MIN/MAX(col - K) -> MIN/MAX(col) - K
     */
    static final class MinMaxAddSubResult extends SimplificationResult {
        private final Literal literal;
        private final boolean isAdd;
        private final ArithmeticOperation originalOp;

        MinMaxAddSubResult(AggregateFunction simplifiedAgg, Literal literal, boolean isAdd, ArithmeticOperation originalOp) {
            super(simplifiedAgg, false);
            this.literal = literal;
            this.isAdd = isAdd;
            this.originalOp = originalOp;
        }

        @Override
        Expression buildCorrection(Attribute aggRef, Attribute countRef) {
            Source src = originalOp.source();
            Configuration config = extractConfiguration(originalOp);
            if (isAdd) {
                return new Add(src, aggRef, literal, config);
            } else {
                return new Sub(src, aggRef, literal, config);
            }
        }
    }

    /**
     * SUM/MIN/MAX(col * K) -> AGG(col) * K
     */
    static final class MulResult extends SimplificationResult {
        private final Literal literal;

        MulResult(AggregateFunction simplifiedAgg, Literal literal) {
            super(simplifiedAgg, false);
            this.literal = literal;
        }

        @Override
        Expression buildCorrection(Attribute aggRef, Attribute countRef) {
            return new Mul(aggRef.source(), aggRef, literal);
        }
    }

    /**
     * Extract the Configuration from an ArithmeticOperation that implements ConfigurationAware (Add, Sub).
     */
    private static Configuration extractConfiguration(ArithmeticOperation op) {
        if (op instanceof Add add) {
            return add.configuration();
        } else if (op instanceof Sub sub) {
            return sub.configuration();
        }
        // Should not happen for Add/Sub cases; Mul doesn't need this
        return null;
    }
}
