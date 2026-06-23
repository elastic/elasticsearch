/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SparklineGenerateEmptyBuckets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * Replaces a {@code SPARKLINE} aggregate into a three-phase execution pipeline that
 * collects per-bucket values, sorts them chronologically, and fills in zero values for
 * any empty time buckets.
 *
 * <p>Given the following query:
 * <pre>
 *     FROM logs
 *     | STATS s = SPARKLINE(MIN(errorCount), @timestamp, 10, "2024-01-01", "2024-02-01"), count = COUNT(*) BY host
 * </pre>
 *
 * The rule produces the following logical plan:
 * <pre>
 *     -- Phase 1: group by time bucket and collect the values
 *     STATS s = MIN(errorCount),
 *           $$count = TO_PARTIAL(COUNT(*))
 *     BY host, $$timestamp = BUCKET(@timestamp, 10, "2024-01-01", "2024-02-01")
 *
 *     -- Phase 2: gather sorted (key, value) arrays per outer group
 *     STATS s          = TOP(s, 100, "asc", $$timestamp),
 *           $$timestamp = TOP($$timestamp, 100, "asc"),
 *           count         = FROM_PARTIAL($$count, COUNT(*))
 *     BY host
 *
 *     -- Phase 3: fill gaps with zeros for buckets that have no data
 *     SparklineGenerateEmptyBuckets(phase2, rounding, minDate, maxDate, ...)
 * </pre>
 *
 * Note: All {@code SPARKLINE} calls within a single {@code STATS} command must share the
 * same {@code timestamp}, {@code buckets}, {@code from}, and {@code to} arguments.
 * At most 100 buckets are supported.
 */
public class ReplaceSparklineAggregate extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    private static final Integer SPARKLINE_BUCKET_LIMIT = 100;

    public ReplaceSparklineAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate plan, LogicalOptimizerContext context) {
        ExtractedAggregates extracted = extractSparklineAggregates(plan);
        if (extracted.sparklineAggregates().isEmpty()) {
            return plan;
        }

        Source source = plan.source();
        FirstPhaseAggregateData firstPhase = buildFirstPhaseAggregate(
            source,
            plan,
            extracted.sparklineAggregates(),
            extracted.nonSparklineAggregates(),
            extracted.dateBucket()
        );
        SecondPhaseAggregateData secondPhase = buildSecondPhaseAggregate(
            source,
            plan,
            extracted.sparklineAggregates(),
            extracted.nonSparklineAggregates(),
            firstPhase
        );
        return buildSparklineGenerateEmptyBuckets(source, plan, extracted.dateBucket(), secondPhase);
    }

    private record ExtractedAggregates(
        List<Map.Entry<Alias, Sparkline>> sparklineAggregates,
        List<Alias> nonSparklineAggregates,
        Bucket dateBucket
    ) {}

    private record FirstPhaseAggregateData(
        LogicalPlan plan,
        List<Alias> sparklineValueAliases,
        List<Alias> toPartialAliases,
        List<AggregateFunction> originalAggFuncs,
        Attribute dateBucketAttribute
    ) {}

    private record SecondPhaseAggregateData(
        Aggregate aggregate,
        List<Alias> topValuesAliases,
        Alias topKeysAlias,
        List<Alias> fromPartialAliases
    ) {}

    private ExtractedAggregates extractSparklineAggregates(Aggregate plan) {
        List<Map.Entry<Alias, Sparkline>> sparklineAggregates = new ArrayList<>();
        List<Alias> nonSparklineAggregates = new ArrayList<>();

        Bucket dateBucket = null;
        for (var aggregate : plan.aggregates()) {
            if (aggregate instanceof Alias alias) {
                Expression unwrapped = Alias.unwrap(alias);
                if (unwrapped instanceof Sparkline s) {
                    Bucket currentBucket = new Bucket(
                        s.source(),
                        s.key(),
                        s.buckets(),
                        s.from(),
                        s.to(),
                        ConfigurationAware.CONFIGURATION_MARKER
                    );
                    if (dateBucket == null) {
                        dateBucket = currentBucket;
                        Object bucketsValue = Foldables.valueOf(FoldContext.small(), s.buckets());
                        if (bucketsValue instanceof Integer bucketCount && bucketCount > SPARKLINE_BUCKET_LIMIT) {
                            throw new IllegalArgumentException(
                                "The buckets argument of SPARKLINE must not exceed " + SPARKLINE_BUCKET_LIMIT
                            );
                        }
                    } else {
                        if (dateBucket.equals(currentBucket) == false) {
                            throw new IllegalArgumentException(
                                "All SPARKLINE functions in a single STATS command must share the same "
                                    + "timestamp, buckets, from, and to values"
                            );
                        }
                    }
                    sparklineAggregates.add(Map.entry(alias, s));
                } else if (unwrapped instanceof AggregateFunction) {
                    nonSparklineAggregates.add(alias);
                }
            }
        }
        return new ExtractedAggregates(sparklineAggregates, nonSparklineAggregates, dateBucket);
    }

    // Builds the first phase aggregate: STATS sparklineValues..., $$toPartials... BY groupings, $$timestamp
    private FirstPhaseAggregateData buildFirstPhaseAggregate(
        Source source,
        Aggregate plan,
        List<Map.Entry<Alias, Sparkline>> sparklineAggregates,
        List<Alias> nonSparklineAggregates,
        Bucket dateBucket
    ) {
        List<Alias> sparklineValueAliases = new ArrayList<>();
        for (Map.Entry<Alias, Sparkline> entry : sparklineAggregates) {
            Alias valAlias = new Alias(source, entry.getKey().name(), entry.getValue().field());
            sparklineValueAliases.add(valAlias);
        }
        List<NamedExpression> firstPhaseAggregates = new ArrayList<>(sparklineValueAliases);

        List<Alias> toPartialAliases = new ArrayList<>();
        List<AggregateFunction> originalAggFuncs = new ArrayList<>();
        for (Alias nonSparkline : nonSparklineAggregates) {
            AggregateFunction aggFunc = (AggregateFunction) Alias.unwrap(nonSparkline);
            ToPartial toPartial = new ToPartial(source, nonSparkline.child(), aggFunc);
            Alias toPartialAlias = new Alias(source, "$$" + nonSparkline.name(), toPartial);
            toPartialAliases.add(toPartialAlias);
            originalAggFuncs.add(aggFunc);
            firstPhaseAggregates.add(toPartialAlias);
        }

        Alias dateBucketAlias = new Alias(source, "$$timestamp", dateBucket);
        Eval dateBucketEval = new Eval(source, plan.child(), List.of(dateBucketAlias));
        Attribute dateBucketAttr = dateBucketAlias.toAttribute();

        List<Expression> firstPhaseGroupings = new ArrayList<>(plan.groupings());
        firstPhaseGroupings.add(dateBucketAttr);

        ParserUtils.Stats firstPhaseStats = ParserUtils.buildStats(source, firstPhaseGroupings, firstPhaseAggregates);
        Aggregate aggregate = new Aggregate(plan.source(), dateBucketEval, firstPhaseStats.groupings(), firstPhaseStats.aggregates());
        // The aggregate-handling rules from the main Substitutions batch (see LogicalPlanOptimizer#substitutions) ran before this rule
        // (it lives after PropagateInlineEvals) and will not run again, yet the first-phase Aggregate we just built embeds the inner
        // aggregation (Sparkline.field(), e.g. COUNT()+1) which those rules never saw. Re-apply just the inner-aggregation lowering subset
        // of that batch so the inner aggregation is lowered like an ordinary STATS:
        // 1. extract scalar expressions nested inside an aggregate (e.g. SUM(SIN(salary))) into a preceding Eval;
        // 2. extract aggregates wrapped in a top-level expression (e.g. COUNT()+1, MIN(a)+MIN(b)) into naked aggs plus a following Eval;
        // 3. expand surrogate aggregates (e.g. AVG -> Div(Sum, Count)). This must run after step 2 so that a surrogate hidden inside an
        // expression (e.g. AVG(x)+1) is first exposed as a naked top-level agg and only then expanded;
        // 4. re-extract any scalar expressions surfaced by surrogate expansion, using locally-unique synthetic names to avoid collisions
        // when the same surrogate also appears standalone in this STATS (e.g. WEIGHTED_AVG used both directly and inside SPARKLINE).
        // This is a subset, not a verbatim copy, of the main batch: RewriteSumOfExpressionPlusConstant (a SUM(x +/- c) optimization) and
        // SubstituteFilteredExpression are intentionally left out -- the former is optional and the latter already ran in the main batch,
        // so any SPARKLINE WHERE filter is already attached to the inner aggregate. The main batch also runs
        // SubstituteSurrogateAggregations twice for a CCS/bwc reason that does not apply to this freshly built local Aggregate, so a
        // single pass is enough here. Without steps 2-3 the physical planner cannot assign a correctly-typed channel to an aggregate that
        // is part of an expression.
        LogicalPlan phase1Plan = new ReplaceAggregateNestedExpressionWithEval().apply(aggregate);
        phase1Plan = new ReplaceAggregateAggExpressionWithEval().apply(phase1Plan);
        phase1Plan = new SubstituteSurrogateAggregations().apply(phase1Plan);
        phase1Plan = new ReplaceAggregateNestedExpressionWithEval(true).apply(phase1Plan);
        return new FirstPhaseAggregateData(phase1Plan, sparklineValueAliases, toPartialAliases, originalAggFuncs, dateBucketAttr);
    }

    // Builds the second phase aggregate STATS sparklineTops..., topKeys, fromPartials... BY groupings
    private SecondPhaseAggregateData buildSecondPhaseAggregate(
        Source source,
        Aggregate plan,
        List<Map.Entry<Alias, Sparkline>> sparklineAggregates,
        List<Alias> nonSparklineAggregates,
        FirstPhaseAggregateData firstPhase
    ) {
        List<NamedExpression> secondPhaseAggregates = new ArrayList<>();

        Attribute dateBucketAttribute = firstPhase.dateBucketAttribute();
        Literal limit = new Literal(source, SPARKLINE_BUCKET_LIMIT, DataType.INTEGER);
        Literal order = new Literal(source, new BytesRef("asc"), DataType.KEYWORD);
        List<Alias> topValuesAliases = new ArrayList<>();
        for (int i = 0; i < firstPhase.sparklineValueAliases().size(); i++) {
            Attribute sparklineValueAttribute = firstPhase.sparklineValueAliases().get(i).toAttribute();
            Top topValuesAggregate = new Top(source, dateBucketAttribute, limit, order, sparklineValueAttribute);
            Alias topValuesAlias = new Alias(
                source,
                sparklineValueAttribute.name(),
                topValuesAggregate,
                sparklineAggregates.get(i).getKey().id()
            );
            topValuesAliases.add(topValuesAlias);
            secondPhaseAggregates.add(topValuesAlias);
        }

        Top topKeysAggregate = new Top(source, dateBucketAttribute, limit, order, null);
        Alias topKeysAlias = new Alias(source, dateBucketAttribute.name(), topKeysAggregate);
        secondPhaseAggregates.add(topKeysAlias);

        List<Alias> fromPartialAliases = new ArrayList<>();
        for (int i = 0; i < firstPhase.toPartialAliases().size(); i++) {
            Attribute partialAttr = firstPhase.toPartialAliases().get(i).toAttribute();
            AggregateFunction originalFunc = firstPhase.originalAggFuncs().get(i);
            FromPartial fromPartial = new FromPartial(source, partialAttr, originalFunc);
            Alias fromPartialAlias = new Alias(
                source,
                nonSparklineAggregates.get(i).name(),
                fromPartial,
                nonSparklineAggregates.get(i).id()
            );
            fromPartialAliases.add(fromPartialAlias);
            secondPhaseAggregates.add(fromPartialAlias);
        }

        List<Attribute> secondPhaseGroupingAttributes = plan.groupings().stream().map(Expressions::attribute).toList();
        ParserUtils.Stats secondPhaseStats = ParserUtils.buildStats(
            source,
            new ArrayList<>(secondPhaseGroupingAttributes),
            new ArrayList<>(secondPhaseAggregates)
        );
        Aggregate aggregate = new Aggregate(plan.source(), firstPhase.plan(), secondPhaseStats.groupings(), secondPhaseStats.aggregates());
        return new SecondPhaseAggregateData(aggregate, topValuesAliases, topKeysAlias, fromPartialAliases);
    }

    // Builds the SparklineGenerateEmptyBuckets plan that fills in zero values for the sparkline graph
    private SparklineGenerateEmptyBuckets buildSparklineGenerateEmptyBuckets(
        Source source,
        Aggregate plan,
        Bucket dateBucket,
        SecondPhaseAggregateData secondPhase
    ) {
        long minDate = foldToLong(FoldContext.small(), dateBucket.from());
        long maxDate = foldToLong(FoldContext.small(), dateBucket.to());
        Rounding.Prepared dateBucketRounding = dateBucket.getDateRounding(FoldContext.small(), minDate, maxDate);
        List<Attribute> passthroughAttributes = secondPhase.fromPartialAliases().stream().map(Alias::toAttribute).toList();
        List<Attribute> topValuesAttributes = secondPhase.topValuesAliases().stream().map(Alias::toAttribute).toList();
        return new SparklineGenerateEmptyBuckets(
            source,
            secondPhase.aggregate(),
            topValuesAttributes,
            secondPhase.topKeysAlias().toAttribute(),
            secondPhase.aggregate().groupings(),
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            buildOutputAttributes(plan.aggregates(), secondPhase.topValuesAliases(), secondPhase.fromPartialAliases())
        );
    }

    private List<Attribute> buildOutputAttributes(
        List<? extends NamedExpression> aggregates,
        List<Alias> sparklineAliases,
        List<Alias> nonSparklineAliases
    ) {
        List<Attribute> outputAttributes = new ArrayList<>();
        int sparklineIndex = 0;
        int nonSparklineIndex = 0;
        for (var aggregate : aggregates) {
            if (aggregate instanceof Alias alias) {
                Expression unwrapped = Alias.unwrap(alias);
                if (unwrapped instanceof Sparkline) {
                    outputAttributes.add(sparklineAliases.get(sparklineIndex++).toAttribute());
                } else if (unwrapped instanceof AggregateFunction) {
                    outputAttributes.add(nonSparklineAliases.get(nonSparklineIndex++).toAttribute());
                }
            } else {
                outputAttributes.add(Expressions.attribute(aggregate));
            }
        }

        return outputAttributes;
    }

    private long foldToLong(FoldContext ctx, Expression e) {
        Object value = Foldables.valueOf(ctx, e);
        return DataType.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }

}
