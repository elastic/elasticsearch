/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class AddTimeRangeToTimeseriesAggregate extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {

        SearchStats searchStats = context.searchStats();
        if (searchStats != null) {
            return plan.transformUp(TimeSeriesAggregate.class, agg -> addTimeRange(agg, searchStats));
        }
        return plan;
    }

    private TimeSeriesAggregate addTimeRange(TimeSeriesAggregate agg, SearchStats searchStats) {
        if (agg.timestampRange() != null) {
            return agg;
        }
        // Nothing explodes catastrophically if we can't apply this optimization, but we still want to make sure it applies always in tests
        // therefore we check with asserts that the optimization is applicable, but still code defensively with ifs for production
        assert agg.timestamp() instanceof FieldAttribute;
        if (agg.timestamp() instanceof FieldAttribute timestampField) {
            FieldAttribute.FieldName fieldName = timestampField.fieldName();
            Object minTimestamp = searchStats.min(fieldName);
            Object maxTimestamp = searchStats.max(fieldName);
            if (minTimestamp == null || maxTimestamp == null) {
                return agg;
            }
            assert minTimestamp instanceof Long && maxTimestamp instanceof Long;
            if (minTimestamp instanceof Long minTs && maxTimestamp instanceof Long maxTs) {
                TimeSeriesAggregate result = agg.withTimestampRange(minTs, maxTs);
                return result;
            }

        }
        return agg;
    }

}
