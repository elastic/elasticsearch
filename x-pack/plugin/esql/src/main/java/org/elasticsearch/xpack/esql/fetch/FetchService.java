/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.io.IOException;
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
     * The most drivers one fetch request runs at the same time on a node, and the most pieces one of its shards splits
     * into. Each piece of a shard is a driver on the worker pool of ES|QL, next to the drivers of the query phase. It bounds
     * the loading only: the node opens the search contexts of every shard of the request before the first driver starts,
     * on the thread that received it, so they all apply the security of the request.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SHARD_TASKS = Setting.intSetting(
        "esql.fetch.max_concurrent_shard_tasks",
        10,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final TransportService transportService;
    private final BlockFactory blockFactory;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final DataNodeFetchExecutor executor;
    private volatile int maxConcurrentShardTasks;

    /**
     * @param blockFactory           the root block factory of the node, which accounts the responses while they are
     *                               serialized, and while a gateway node reads one it forwards to a node of another version
     * @param namedWriteableRegistry reads a response that a gateway node forwards to a node of another version
     * @param driverExecutors        runs the drivers that load the documents, next to the drivers of the query phase
     * @param plannerFactory         builds the planner of each fetch request
     */
    public FetchService(
        TransportService transportService,
        ClusterService clusterService,
        SearchService searchService,
        BlockFactory blockFactory,
        NamedWriteableRegistry namedWriteableRegistry,
        FetchDriverExecutors driverExecutors,
        Supplier<PlannerSettings> plannerSettings,
        FetchPlannerFactory plannerFactory
    ) {
        this.transportService = transportService;
        this.blockFactory = blockFactory;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.executor = new DataNodeFetchExecutor(
            transportService,
            clusterService,
            searchService,
            blockFactory,
            driverExecutors,
            plannerSettings,
            plannerFactory,
            () -> maxConcurrentShardTasks
        );
        clusterService.getClusterSettings().initializeAndWatch(MAX_CONCURRENT_SHARD_TASKS, max -> maxConcurrentShardTasks = max);
    }

    /**
     * Sends a fetch request to {@code target}, the node {@code node} names, as a child of the task of the query. The
     * response is read on the search pool, into the block factory of this node. A request to this node is handed over
     * without serialization, through the same handler and the same authorization. A request to a node of a remote cluster
     * goes through a gateway node of that cluster when this node isn't connected to it.
     *
     * @throws org.elasticsearch.transport.NodeNotConnectedException when this node can't reach a node of its own cluster
     */
    public void sendFetch(
        FetchNode node,
        DiscoveryNode target,
        FetchRequest request,
        Task parentTask,
        ActionListener<FetchResponse> listener
    ) {
        transportService.sendChildRequest(
            FetchNodeDirectory.connection(transportService, node, target),
            FETCH_ACTION_NAME,
            request,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                this::readResponse,
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            )
        );
    }

    private FetchResponse readResponse(StreamInput in) throws IOException {
        // the stream keeps a reference to the last block of nulls it read, which closing it releases
        try (BlockStreamInput blocks = new BlockStreamInput(in, blockFactory)) {
            return new FetchResponse(blocks, transportService.getThreadPool().getThreadContext());
        }
    }

    /**
     * Serves fetch requests on the search pool, where reader contexts open their searchers and document level security
     * can run queries. A gateway node forwards the fetch requests of a coordinator of another cluster that isn't connected
     * to the node they go to.
     */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            FETCH_ACTION_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
            FetchRequest::new,
            (request, channel, task) -> executor.execute(request, (CancellableTask) task, new ChannelActionListener<>(channel))
        );
        TransportActionProxy.registerProxyAction(transportService, FETCH_ACTION_NAME, true, this::readResponse, namedWriteableRegistry);
    }
}
