/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.physical.AbstractAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNAggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

import java.util.List;

interface PhysicalOperationProviders {
    PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source);

    PhysicalOperation sourcePhysicalOperation(EsQueryExec esQuery, LocalExecutionPlannerContext context);

    PhysicalOperation timeSeriesSourceOperation(TimeSeriesSourceExec ts, LocalExecutionPlannerContext context);

    PhysicalOperation groupingPhysicalOperation(
        AbstractAggregateExec aggregateExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    );
}
