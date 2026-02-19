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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class ReplaceSparklineAggregate extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    public ReplaceSparklineAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate plan, LogicalOptimizerContext context) {
        // TODO: Can we merge sparklineAliases and sparklines into a list of tuples? Or some cleaner structure?
        List<AbstractMap.SimpleEntry<String, Sparkline>> sparklineAggregates = new ArrayList<>();
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
                    sparklineAggregates.add(new AbstractMap.SimpleEntry<>(alias.name(), s));
                } else if (unwrapped instanceof AggregateFunction) {
                    nonSparklineAggregates.add(alias);
                }
            }
        }

        if (sparklineAggregates.isEmpty()) {
            return plan;
        }

        Source source = plan.source(); // TODO: Decide if we want to use a different source

        // Phase 1: STATS sparklineValues..., $$toPartials... BY groupings, $$timestamp
        List<Alias> sparklineValueAliases = new ArrayList<>();
        for (AbstractMap.SimpleEntry<String, Sparkline> entry : sparklineAggregates) {
            Alias valAlias = new Alias(source, entry.getKey(), entry.getValue().field());
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
        Aggregate firstPhaseAggregate = new Aggregate(
            plan.source(),
            plan.child(),
            firstPhaseStats.groupings(),
            firstPhaseStats.aggregates()
        );

        // Phase 2: STATS sparklineTops..., topKeys, fromPartials... BY groupings
        List<NamedExpression> secondPhaseAggregates = new ArrayList<>();

        Attribute dateBucketAttribute = dateBucketAlias.toAttribute();
        // TODO: Decide on this limit value or if it's even needed
        Literal limit = new Literal(source, 50, DataType.INTEGER);
        Literal order = new Literal(source, new BytesRef("asc"), DataType.KEYWORD);
        List<Alias> topValuesAliases = new ArrayList<>();
        for (Alias sparklineValueAlias : sparklineValueAliases) {
            Attribute sparklineValueAttribute = sparklineValueAlias.toAttribute();
            Top topValuesAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, sparklineValueAttribute);
            Alias topValuesAlias = new Alias(Source.EMPTY, sparklineValueAttribute.name(), topValuesAggregate);
            topValuesAliases.add(topValuesAlias);
            secondPhaseAggregates.add(topValuesAlias);
        }

        Top topKeysAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, null);
        Alias topKeysAlias = new Alias(Source.EMPTY, dateBucketAttribute.name(), topKeysAggregate);
        secondPhaseAggregates.add(topKeysAlias);

        List<Alias> fromPartialAliases = new ArrayList<>();
        for (int i = 0; i < toPartialAliases.size(); i++) {
            Attribute partialAttr = toPartialAliases.get(i).toAttribute();
            AggregateFunction originalFunc = originalAggFuncs.get(i);
            FromPartial fromPartial = new FromPartial(source, partialAttr, originalFunc);
            Alias fromPartialAlias = new Alias(source, nonSparklineAggregates.get(i).name(), fromPartial);
            fromPartialAliases.add(fromPartialAlias);
            secondPhaseAggregates.add(fromPartialAlias);
        }

        List<Attribute> secondPhaseGroupingAttributes = plan.groupings().stream().map(Expressions::attribute).toList();
        Stats secondPhaseStats = stats(
            Source.EMPTY,
            new ArrayList<>(secondPhaseGroupingAttributes),
            new ArrayList<>(secondPhaseAggregates)
        );
        Aggregate secondPhaseAggregate = new Aggregate(
            plan.source(),
            firstPhaseAggregate,
            secondPhaseStats.groupings(),
            secondPhaseStats.aggregates()
        );

        long minDate = foldToLong(FoldContext.small(), dateBucket.from());
        long maxDate = foldToLong(FoldContext.small(), dateBucket.to());
        Rounding.Prepared dateBucketRounding = dateBucket.getDateRounding(FoldContext.small(), minDate, maxDate);
        List<Attribute> passthroughAttributes = fromPartialAliases.stream().map(Alias::toAttribute).toList();
        List<Attribute> topValuesAttributes = topValuesAliases.stream().map(Alias::toAttribute).toList();
        return new SparklineGenerateEmptyBuckets(
            source,
            secondPhaseAggregate,
            topValuesAttributes,
            topKeysAlias.toAttribute(),
            secondPhaseAggregate.groupings(),
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            buildOutputAttributes(plan.aggregates(), topValuesAliases, fromPartialAliases)
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
