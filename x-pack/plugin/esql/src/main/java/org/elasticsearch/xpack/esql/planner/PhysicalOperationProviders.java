/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

interface PhysicalOperationProviders {
    PhysicalOperation getFieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source);

    PhysicalOperation getSourcePhysicalOperation(EsQueryExec esQuery, LocalExecutionPlannerContext context);

    PhysicalOperation getGroupingPhysicalOperation(
        AggregateExec aggregateExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    );
}
