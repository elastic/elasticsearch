/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Context for creating an aggregate-scan operator factory. Mirrors the role of
 * {@link SourceOperatorContext} but for the runtime aggregate-pushdown path used by
 * {@code ExternalAggregatePushdownExec}: each split is dispatched to a driver, the operator
 * iterates the format reader's {@link AggregateScanReader#scanForAggregates} pages, and
 * forwards them to the FINAL aggregator above.
 * <p>
 * This context intentionally omits scan-only fields (batch size, push filter, error policy,
 * row limit, parsing parallelism). Filter pushdown does not compose with aggregate pushdown
 * today (the planner rule is gated by {@code aggregatePushdownSupport().canPushAggregates}
 * which already returns NO when a filter has been pushed onto the reader).
 */
public record AggregateScanOperatorContext(
    String sourceType,
    StoragePath path,
    Map<String, Object> config,
    ExternalSliceQueue sliceQueue,
    List<NamedExpression> aggregates,
    List<Attribute> intermediateAttributes,
    Executor executor
) {
    public AggregateScanOperatorContext {
        Check.notNull(sourceType, "sourceType cannot be null");
        Check.notNull(path, "path cannot be null");
        Check.notNull(sliceQueue, "sliceQueue cannot be null");
        Check.notNull(executor, "executor cannot be null");
        config = config != null ? Map.copyOf(config) : Map.of();
        aggregates = List.copyOf(aggregates);
        intermediateAttributes = List.copyOf(intermediateAttributes);
    }
}
