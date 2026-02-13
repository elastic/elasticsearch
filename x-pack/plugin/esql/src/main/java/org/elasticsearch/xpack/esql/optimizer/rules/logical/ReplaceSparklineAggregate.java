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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
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

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class ReplaceSparklineAggregate extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    public ReplaceSparklineAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate plan, LogicalOptimizerContext context) {
        for (var aggregate : plan.aggregates()) {
            for (var expression : aggregate.children()) {
                if (expression instanceof Sparkline sparkline) {
                    Bucket dateBucket = new Bucket(
                        sparkline.source(),
                        sparkline.key(),
                        sparkline.buckets(),
                        sparkline.from(),
                        sparkline.to(),
                        ConfigurationAware.CONFIGURATION_MARKER
                    );

                    Alias dateBucketAlias = new Alias(sparkline.source(), "date_bucket", dateBucket);
                    Attribute dateBucketAttribute = dateBucketAlias.toAttribute();
                    Alias valueAlias = new Alias(sparkline.source(), aggregate.name(), sparkline.field());
                    Attribute valueAttribute = valueAlias.toAttribute();

                    // TODO: Decide on this limit value or if it's even needed
                    Literal limit = new Literal(sparkline.source(), 50, DataType.INTEGER);
                    Literal order = new Literal(sparkline.source(), new BytesRef("asc"), DataType.KEYWORD);

                    // Calculate | STATS value=value BY groupings, buckets
                    List<Expression> firstPhaseGroupings = new ArrayList<>(plan.groupings());
                    firstPhaseGroupings.add(dateBucketAlias);
                    List<NamedExpression> firstPhaseAggregates = List.of(valueAlias);
                    Stats firstPhaseStats = stats(sparkline.source(), firstPhaseGroupings, new ArrayList<>(firstPhaseAggregates));
                    Aggregate firstPhaseAggregate = new Aggregate(
                        plan.source(),
                        plan.child(),
                        firstPhaseStats.groupings(),
                        firstPhaseStats.aggregates()
                    );

                    // Calculate | STATS Top(key, 50, 'asc', value), Top(key, 50, 'asc', value) BY grouping
                    Top topKeysAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, null);
                    Alias topKeysAttribute = new Alias(Source.EMPTY, dateBucketAttribute.name(), topKeysAggregate);
                    Top topValuesAggregate = new Top(Source.EMPTY, dateBucketAttribute, limit, order, valueAttribute);
                    Alias topValuesAttribute = new Alias(Source.EMPTY, valueAttribute.name(), topValuesAggregate);
                    List<Alias> secondPhaseAggregates = List.of(topValuesAttribute, topKeysAttribute);
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

                    // Build a SparklinePostProcessing logical plan to fill in the empty buckets
                    // TODO: SparklineGenerateEmptyBuckets may not be the best name?
                    long minDate = foldToLong(FoldContext.small(), dateBucket.from());
                    long maxDate = foldToLong(FoldContext.small(), dateBucket.to());
                    Rounding.Prepared dateBucketRounding = dateBucket.getDateRounding(FoldContext.small(), minDate, maxDate);
                    return new SparklineGenerateEmptyBuckets(
                        sparkline.source(),
                        secondPhaseAggregate,
                        topValuesAttribute.toAttribute(),
                        topKeysAttribute.toAttribute(),
                        secondPhaseAggregate.groupings(),
                        dateBucketRounding,
                        minDate,
                        maxDate
                    );
                }
            }
        }
        return plan;
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
