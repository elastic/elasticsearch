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

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;

/**
 * Extracts the time range a {@link TimeSeriesAggregate} operates on from the search stats and adds it to the aggregation
 * if possible. This allows us to optimize the time rounding, e.g. for timezones with daylight saving time changes.
 */
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
        if (agg.timestamp() instanceof FieldAttribute timestampField && isDateTime(timestampField.dataType())) {
            FieldAttribute.FieldName fieldName = timestampField.fieldName();
            Object minTimestamp = searchStats.min(fieldName);
            Object maxTimestamp = searchStats.max(fieldName);
            if (minTimestamp == null || maxTimestamp == null) {
                return agg;
            }
            // Fail in tests, silently ignore in production as this optimization is not mission critical
            assert minTimestamp instanceof Long && maxTimestamp instanceof Long;
            if (minTimestamp instanceof Long minTs && maxTimestamp instanceof Long maxTs) {
                TimeSeriesAggregate result = agg.withTimestampRange(minTs, maxTs);
                return result;
            }

        }
        return agg;
    }

}
