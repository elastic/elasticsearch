/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.fetch.lifetime.FetchContextService;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * The fetch action of this node: it loads the fetched columns of the documents this node holds, at the request of a
 * coordinator. The handler is registered whether the fetch phase is on or not, so a node answers even when the feature
 * flags of the nodes differ.
 */
public final class FetchService {
    /**
     * Loads documents on the node that holds them. Its requests carry the index expressions of the query, so they are
     * authorized like the query's other index actions.
     */
    public static final String FETCH_ACTION_NAME = EsqlQueryAction.NAME + "/fetch";

    /**
     * The most shards one fetch request loads at the same time on a node. Each shard is a driver on the worker pool of
     * ES|QL, next to the drivers of the query phase. It bounds the loading only: the node opens the search contexts of
     * every shard of the request before the first driver starts, on the thread that received it, so they all apply the
     * security of the request.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SHARD_TASKS = Setting.intSetting(
        "esql.fetch.max_concurrent_shard_tasks",
        10,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final TransportService transportService;
    private final DataNodeFetchExecutor executor;
    private volatile int maxConcurrentShardTasks;

    /**
     * @param contextService frees the contexts a request names once the node answered it
     * @param blockFactory   the root block factory of the node, which accounts the responses while they are serialized
     * @param driverExecutor runs the drivers that load the documents, next to the drivers of the query phase
     * @param plannerFactory builds the planner of each fetch request
     */
    public FetchService(
        TransportService transportService,
        ClusterService clusterService,
        SearchService searchService,
        FetchContextService contextService,
        BlockFactory blockFactory,
        Executor driverExecutor,
        Supplier<PlannerSettings> plannerSettings,
        FetchPlannerFactory plannerFactory
    ) {
        this.transportService = transportService;
        this.executor = new DataNodeFetchExecutor(
            transportService,
            clusterService,
            searchService,
            contextService,
            blockFactory,
            driverExecutor,
            plannerSettings,
            plannerFactory,
            () -> maxConcurrentShardTasks
        );
        clusterService.getClusterSettings().initializeAndWatch(MAX_CONCURRENT_SHARD_TASKS, max -> maxConcurrentShardTasks = max);
    }

    /**
     * Serves fetch requests on the search pool, where reader contexts open their searchers and document level security
     * can run queries.
     */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            FETCH_ACTION_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
            FetchRequest::new,
            (request, channel, task) -> executor.execute(request, (CancellableTask) task, new ChannelActionListener<>(channel))
        );
    }
}
