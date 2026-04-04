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
        // Since this rule has to occur after PropogateInlineEvals to work with INLINE STATS, we don't get surrogate substitution
        // to handle inner aggregates that are SurrogateExpressions (e.g., AVG → Div(Sum, Count)). We apply the substitution here to ensure
        // that any inner aggregates are properly replaced with their surrogates in the first phase plan.
        LogicalPlan phase1Plan = new SubstituteSurrogateAggregations().apply(aggregate);
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
