/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Map;

public final class UseSinglePassTimeSeriesAggregate extends OptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregate,
    LogicalOptimizerContext> {

    public UseSinglePassTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
        Boolean querySetting = QuerySettings.TIME_SERIES_REDUCE_PER_INDEX.get(context.configuration().resolvedSettings());
        boolean reducePerIndex = querySetting != null
            ? querySetting
            : context.configuration().pragmas().timeSeriesReducePerIndex(context.flags().timeSeriesReducePerIndex());
        if (aggregate.singlePass() == false
            && context.configuration().pragmas().timeSeriesSinglePass()
            && context.minimumVersion().supports(TimeSeriesAggregate.TIME_SERIES_SINGLE_PASS)
            && canExecuteSinglePass(aggregate, reducePerIndex)) {
            return aggregate.withSinglePass();
        }
        return aggregate;
    }

    private static boolean canExecuteSinglePass(TimeSeriesAggregate aggregate, boolean reducePerIndex) {
        List<EsRelation> relations = aggregate.collect(EsRelation.class);
        if (relations.size() != 1) {
            return false;
        }
        Map<String, IndexProperties> indexProperties = relations.getFirst().indexProperties();
        if (indexProperties.isEmpty() || (reducePerIndex == false && indexProperties.size() != 1)) {
            return false;
        }
        for (IndexProperties properties : indexProperties.values()) {
            int createdVersionId = properties.createdVersion();
            if (createdVersionId == 0 || IndexVersion.fromId(createdVersionId).before(IndexVersions.WRITE_TSID_PREFIX_PARTITION)) {
                return false;
            }
        }
        boolean requiredSorted = false;
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                if (af.hasWindow() && reducePerIndex == false) {
                    return false;
                }
                requiredSorted |= (af instanceof TimeSeriesAggregateFunction ts && ts.requiredTimeSeriesSource());
            }
        }
        return requiredSorted;
    }
}
