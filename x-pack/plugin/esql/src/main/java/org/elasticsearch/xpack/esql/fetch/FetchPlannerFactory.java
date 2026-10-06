/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.planner.FetchSourceProvider;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Builds the planner of one fetch request with the services of this node, the way the query phase builds its planners.
 */
@FunctionalInterface
public interface FetchPlannerFactory {
    /**
     * @param shardContexts the shards of the request, which the operators that load fields read from
     * @param fetchSources  the source of the fetch plan
     */
    LocalExecutionPlanner create(
        String sessionId,
        String clusterAlias,
        CancellableTask task,
        Configuration configuration,
        FoldContext foldCtx,
        IndexedByShardId<? extends ShardContext> shardContexts,
        FetchSourceProvider fetchSources
    );
}
