/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Results from running a chunk of ESQL.
 * @param schema "Schema" of the {@link Attribute}s that are produced by the {@link LogicalPlan}
 *               that was run. Each {@link Page} contains a {@link Block} of values for each
 *               attribute in this list.
 * @param pages Actual values produced by running the ESQL.
 * @param completionInfo Information collected from drivers after they've been completed.
 * @param executionInfo Metadata about the execution of this query. Used for cross cluster queries.
 */
public record Result(
    List<Attribute> schema,
    List<Page> pages,
    DriverCompletionInfo completionInfo,
    @Nullable EsqlExecutionInfo executionInfo
) {}
