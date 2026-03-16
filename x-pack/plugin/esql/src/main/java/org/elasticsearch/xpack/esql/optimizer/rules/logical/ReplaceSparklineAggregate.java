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
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SparklineGenerateEmptyBuckets;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class ReplaceSparklineAggregate extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

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
        Aggregate aggregate,
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
                    } else {
                        if (dateBucket.equals(currentBucket) == false) {
                            throw new ParsingException(
                                alias.source(),
                                "All SPARKLINE functions in a single STATS command must share the same "
                                    + "timestamp, bucket_count, from, and to values"
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

        List<Expression> firstPhaseGroupings = new ArrayList<>(plan.groupings());
        Alias dateBucketAlias = new Alias(source, "$$timestamp", dateBucket);
        firstPhaseGroupings.add(dateBucketAlias);

        Stats firstPhaseStats = stats(source, firstPhaseGroupings, firstPhaseAggregates);
        Aggregate aggregate = new Aggregate(plan.source(), plan.child(), firstPhaseStats.groupings(), firstPhaseStats.aggregates());
        return new FirstPhaseAggregateData(
            aggregate,
            sparklineValueAliases,
            toPartialAliases,
            originalAggFuncs,
            dateBucketAlias.toAttribute()
        );
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
        // TODO: Decide on this limit value or if it's even needed
        Literal limit = new Literal(source, 50, DataType.INTEGER);
        Literal order = new Literal(source, new BytesRef("asc"), DataType.KEYWORD);
        List<Alias> topValuesAliases = new ArrayList<>();
        for (int i = 0; i < firstPhase.sparklineValueAliases().size(); i++) {
            Attribute sparklineValueAttribute = firstPhase.sparklineValueAliases().get(i).toAttribute();
            Top topValuesAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, sparklineValueAttribute);
            Alias topValuesAlias = new Alias(
                Source.EMPTY,
                sparklineValueAttribute.name(),
                topValuesAggregate,
                sparklineAggregates.get(i).getKey().id()
            );
            topValuesAliases.add(topValuesAlias);
            secondPhaseAggregates.add(topValuesAlias);
        }

        Top topKeysAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, null);
        Alias topKeysAlias = new Alias(Source.EMPTY, dateBucketAttribute.name(), topKeysAggregate);
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
        Stats secondPhaseStats = stats(
            Source.EMPTY,
            new ArrayList<>(secondPhaseGroupingAttributes),
            new ArrayList<>(secondPhaseAggregates)
        );
        Aggregate aggregate = new Aggregate(
            plan.source(),
            firstPhase.aggregate(),
            secondPhaseStats.groupings(),
            secondPhaseStats.aggregates()
        );
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

    // This foldToLong is mostly copied from Bucket. We likely want to:
    // 1. Remove any logic we don't need here and keep a simpler version for sparklines
    // 2. Move some of this logic to shared code
    private long foldToLong(FoldContext ctx, Expression e) {
        Object value = Foldables.valueOf(ctx, e);
        return DataType.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }

    // This Stats record and building function are mostly copied from LogicalPlanBuilder as it's private there. We likely want to:
    // 1. Remove any logic we don't need here and keep a simpler version for sparklines
    // 2. Move some of this logic to shared code
    public record Stats(List<Expression> groupings, List<? extends NamedExpression> aggregates) {}

    public static Stats stats(Source source, List<Expression> groupings, List<NamedExpression> aggregates) {
        if (aggregates.isEmpty() && groupings.isEmpty()) {
            throw new ParsingException(source, "At least one aggregation or grouping expression required in [{}]", source.text());
        }
        // grouping keys are automatically added as aggregations however the user is not allowed to specify them
        if (groupings.isEmpty() == false && aggregates.isEmpty() == false) {
            var groupNames = new LinkedHashSet<>(Expressions.names(groupings));
            var groupRefNames = new LinkedHashSet<>(Expressions.names(Expressions.references(groupings)));

            for (Expression aggregate : aggregates) {
                Expression e = Alias.unwrap(aggregate);
                if (e.resolved() == false && e instanceof UnresolvedFunction == false) {
                    String name = e.sourceText();
                    if (groupNames.contains(name)) {
                        fail(e, "grouping key [{}] already specified in the STATS BY clause", name);
                    } else if (groupRefNames.contains(name)) {
                        fail(e, "Cannot specify grouping expression [{}] as an aggregate", name);
                    }
                }
            }
        }
        // since groupings are aliased, add refs to it in the aggregates
        for (Expression group : groupings) {
            aggregates.add(Expressions.attribute(group));
        }
        return new Stats(new ArrayList<>(groupings), aggregates);
    }
}
