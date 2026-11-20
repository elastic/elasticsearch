/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverExecutor;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * The fetch action of this node: it loads the fetched columns of the documents this node holds, at the request of a
 * coordinator. The handler is registered whether the fetch phase is on or not, so a node answers even when the feature
 * flags of the nodes differ.
 * <p>
 * For each shard of a request it finds the reader context the query phase kept open and builds a search context on it,
 * on the thread that received the request, so the searcher applies the document and field level security of that request.
 * Then one driver loads the documents of every shard, in request order. A request that fails for one shard fails as a
 * whole, like a fetch request of {@code _search}.
 * <p>
 * The request holds the first reference on the context of each shard, and the pages take their own while they load. It
 * drops its references once the response is sent, because dropping the last reference to a reader can block. Before that
 * it frees the contexts the request reads for the last time, like the fetch phase of {@code _search} frees the context it
 * read.
 * <p>
 * On the coordinator of a query, it builds the fetch operators that send these requests, see {@link #operatorProvider}.
 */
public final class FetchService {
    private static final Logger logger = LogManager.getLogger(FetchService.class);

    /**
     * Loads documents on the node that holds them. Its requests carry the index expressions of the query, which security
     * resolves to what the user may read. The node refuses a request for a shard outside them.
     */
    public static final String FETCH_ACTION_NAME = EsqlQueryAction.NAME + "/fetch";

    /**
     * Describes the driver that loads the fetched columns, in profiles.
     */
    public static final String DESCRIPTION = "fetch";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SearchService searchService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final Supplier<PlannerSettings> plannerSettings;
    private final DriverExecutor driverExecutor;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    /**
     * @param indexNameExpressionResolver resolves the indices a request names, to check that they hold its shards
     * @param blockFactory   the root block factory of the node. Its breaker charges a serialized response until it is
     *                       written.
     * @param driverExecutor runs the driver that loads the documents, on the worker pool of ES|QL next to the drivers of the
     *                       query phase
     */
    public FetchService(
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchService searchService,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        Supplier<PlannerSettings> plannerSettings,
        DriverExecutor driverExecutor
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.searchService = searchService;
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.plannerSettings = plannerSettings;
        this.driverExecutor = driverExecutor;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
    }

    /**
     * Sends a fetch request to {@code target}, a node of this cluster, as a child of the task of the query. The response is
     * read on the search pool, into the block factory of this node. A request to this node is handed over without
     * serialization, through the same handler and the same authorization.
     */
    public void sendFetch(DiscoveryNode target, FetchRequest request, Task parentTask, ActionListener<FetchResponse> listener) {
        sendFetch(transportService, blockFactory, target, request, parentTask, listener);
    }

    /**
     * {@link #sendFetch(DiscoveryNode, FetchRequest, Task, ActionListener)} without the rest of the service, so the tests of
     * the fetch operator send their requests the same way.
     *
     * @param blockFactory reads the blocks of the response
     */
    static void sendFetch(
        TransportService transportService,
        BlockFactory blockFactory,
        DiscoveryNode target,
        FetchRequest request,
        Task parentTask,
        ActionListener<FetchResponse> listener
    ) {
        transportService.sendChildRequest(
            target,
            FETCH_ACTION_NAME,
            request,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                in -> readResponse(in, blockFactory, transportService.getThreadPool().getThreadContext()),
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            )
        );
    }

    private static FetchResponse readResponse(StreamInput in, BlockFactory blockFactory, ThreadContext threadContext) throws IOException {
        // the stream keeps a reference to the last block of nulls it read, which closing it releases
        try (BlockStreamInput blocks = new BlockStreamInput(in, blockFactory)) {
            return new FetchResponse(blocks, threadContext);
        }
    }

    /**
     * The fetch operators of the query that {@code scope} describes, on its coordinator. Each sends its requests through
     * this service to the nodes of the local cluster, with the index expressions of its {@link FetchExec} and the indices
     * options of the query's data node requests.
     */
    public FetchOperatorProvider operatorProvider(QueryFetchScope scope) {
        return new FetchOperatorProvider() {
            @Override
            public Operator.OperatorFactory fetchOperator(FetchExec exec, int docChannel) {
                OriginalIndices indices = new OriginalIndices(
                    exec.originalIndices().toArray(String[]::new),
                    SearchRequest.DEFAULT_INDICES_OPTIONS
                );
                return FetchOperator.Factory.forExec(
                    exec,
                    docChannel,
                    new QueryFetchClient(
                        FetchService.this::sendFetch,
                        clusterService.threadPool().getThreadContext(),
                        () -> clusterService.state().nodes(),
                        scope,
                        indices
                    )
                );
            }

            @Override
            public boolean receivesDetachedDocuments() {
                return true;
            }
        };
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
            // the serialized response stays charged until it is written, like a page of the exchange
            (request, channel, task) -> execute(
                request,
                (CancellableTask) task,
                new ChannelActionListener<>(channel, blockFactory.breaker())
            )
        );
    }

    /**
     * Loads the documents of {@code request} on this node. Runs on the thread that received it, whose thread context holds
     * the authorization of the request.
     */
    void execute(FetchRequest request, CancellableTask task, ActionListener<FetchResponse> listener) {
        new Execution(request, task, listener).start();
    }

    /**
     * One fetch request, from receiving it to dropping the references of its shards.
     */
    private final class Execution {
        private final long startNanos = System.nanoTime();
        private final FetchRequest request;
        private final CancellableTask task;
        private final ActionListener<FetchResponse> listener;
        /**
         * The context of each shard the request opened, in request order. A request that fails to open a shard opens no
         * further one.
         */
        private final List<DefaultShardContext> contexts;
        private final AtomicBoolean released = new AtomicBoolean();

        Execution(FetchRequest request, CancellableTask task, ActionListener<FetchResponse> listener) {
            this.request = request;
            this.task = task;
            this.listener = listener;
            this.contexts = new ArrayList<>(request.shards().size());
        }

        void start() {
            // the driver adds its pages on its own thread, and the response reads them once it finished
            List<Page> pages = Collections.synchronizedList(new ArrayList<>());
            Driver driver;
            try {
                task.ensureNotCancelled();
                ensureTheIndicesHoldEveryShard();
                Set<String> unmappedFieldNames = unmappedFieldNames(request.attributes());
                for (int position = 0; position < request.shards().size(); position++) {
                    contexts.add(openShard(position, request.shards().get(position), unmappedFieldNames));
                }
                driver = createDriver(pages);
            } catch (Exception e) {
                fail(e);
                return;
            }
            long setupNanos = System.nanoTime() - startNanos;
            // runs at once when the task is already cancelled, and the driver fails on its first iteration
            task.addListener(() -> driver.cancel(Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled")));
            // not ActionListener#wrap: when respond throws, wrap would fail the request again and release the pages twice
            Driver.start(
                transportService.getThreadPool().getThreadContext(),
                driverExecutor,
                driver,
                Driver.DEFAULT_MAX_ITERATIONS,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void ignored) {
                        respond(pages, driver, setupNanos);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Releasables.closeExpectNoException(pages);
                        fail(e);
                    }
                }
            );
        }

        /**
         * Security replaced the index expressions of the request with the indices and aliases the user may read, and the
         * searcher of a shard of another index would read it without the document and field level security of that index.
         * That happens when an alias the query read through moved to other indices before the fetch. So such a shard fails
         * the request.
         */
        private void ensureTheIndicesHoldEveryShard() {
            String[] resolved = indexNameExpressionResolver.concreteIndexNames(
                projectResolver.getProjectMetadata(clusterService.state()),
                request
            );
            Set<String> indices = new HashSet<>(Arrays.asList(resolved));
            for (FetchRequest.ShardDocs shard : request.shards()) {
                if (indices.contains(shard.shardId().getIndexName()) == false) {
                    throw new IllegalArgumentException(
                        "shard "
                            + shard.shardId()
                            + " isn't in the indices "
                            + Arrays.toString(request.indices())
                            + " of the fetch request, an alias the query read through may have moved"
                    );
                }
            }
        }

        private DefaultShardContext openShard(int position, FetchRequest.ShardDocs shard, Set<String> unmappedFieldNames)
            throws IOException {
            ReaderContext readerContext = searchService.findReaderContext(shard.contextId(), request, shard.shardId());
            ShardSearchRequest shardRequest = new ShardSearchRequest(
                shard.shardId(),
                request.configuration().absoluteStartedTimeInMillis(),
                AliasFilter.EMPTY,
                request.clusterAlias(),
                SplitShardCountSummary.UNSET
            );
            SearchContext searchContext = searchService.createSearchContext(readerContext, shardRequest, SearchService.NO_TIMEOUT);
            boolean success = false;
            try {
                // reads the fields as the query phase did, a name under a flattened field included
                EsqlSearchExecutionContext executionContext = new EsqlSearchExecutionContext(
                    searchContext.getSearchExecutionContext(),
                    QueryWarnings.EMIT,
                    unmappedFieldNames,
                    request.configuration().flattenedKeys()
                );
                searchContext.addReleasable(executionContext::releaseQueryConstructionMemory);
                // the request's reference, the first on the context. The search context closes once every reference is gone.
                DefaultShardContext context = new DefaultShardContext(position, searchContext, executionContext, AliasFilter.EMPTY);
                success = true;
                return context;
            } finally {
                if (success == false) {
                    searchContext.close();
                }
            }
        }

        /**
         * One driver that emits the documents of every shard in request order, loads the attributes of the request for them
         * and drops the {@code _doc} column.
         */
        private Driver createDriver(List<Page> pages) {
            IndexedByShardIdFromList<DefaultShardContext> shardContexts = new IndexedByShardIdFromList<>(contexts);
            List<FetchDocsSourceOperator.ShardDocs> docs = new ArrayList<>(request.shards().size());
            for (int position = 0; position < request.shards().size(); position++) {
                FetchRequest.ShardDocs shard = request.shards().get(position);
                docs.add(new FetchDocsSourceOperator.ShardDocs(position, shard.segments(), shard.docs()));
            }
            EsPhysicalOperationProviders operationProviders = new EsPhysicalOperationProviders(
                request.configuration().newFoldContext(),
                request.configuration(),
                shardContexts,
                searchService.getIndicesService().getAnalysis(),
                plannerSettings.get(),
                searchService.getIndicesService()::currentStoreBytesRead,
                QueryWarnings.EMIT
            );
            LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
                blockFactory.breaker(),
                localBreakerSettings.overReservedBytes(),
                localBreakerSettings.maxOverReservedBytes()
            );
            List<Releasable> built = new ArrayList<>();
            built.add(localBreaker);
            boolean success = false;
            try {
                DriverContext driverContext = new DriverContext(
                    bigArrays,
                    blockFactory.newChildFactory(localBreaker),
                    localBreakerSettings,
                    DESCRIPTION
                );
                FetchDocsSourceOperator source = new FetchDocsSourceOperator(driverContext.blockFactory(), shardContexts, docs);
                built.add(source);
                Operator reader = operationProviders.valuesSourceReader(request.attributes(), 0).get(driverContext);
                built.add(reader);
                Operator dropDocs = new ProjectOperator(IntStream.rangeClosed(1, request.attributes().size()).boxed().toList());
                built.add(dropDocs);
                OutputOperator output = new OutputOperator(List.of(), Function.identity(), page -> {
                    // the response releases the page on another thread
                    page.allowPassingToDifferentDriver();
                    pages.add(page);
                });
                Driver driver = new Driver(
                    new TaskId(clusterService.localNode().getId(), task.getId()).toString(),
                    DESCRIPTION,
                    clusterService.getClusterName().value(),
                    clusterService.getNodeName(),
                    System.currentTimeMillis(),
                    System.nanoTime(),
                    driverContext,
                    request::getDescription,
                    source,
                    List.of(reader, dropDocs),
                    output,
                    request.configuration().pragmas().statusInterval(),
                    localBreaker
                );
                success = true;
                return driver;
            } finally {
                if (success == false) {
                    // the driver never existed, so nothing else closes them
                    Releasables.closeExpectNoException(built);
                }
            }
        }

        private void respond(List<Page> pages, Driver driver, long setupNanos) {
            FetchResponse response;
            try {
                response = new FetchResponse(pages, completionInfo(driver), System.nanoTime() - startNanos, setupNanos);
            } catch (Exception e) {
                Releasables.closeExpectNoException(pages);
                fail(e);
                return;
            }
            // the coordinator places the rows of the response by their position, so every document needs one row
            assert response.rows() == request.totalDocs()
                : "fetched [" + response.rows() + "] rows for [" + request.totalDocs() + "] documents";
            logger.debug(
                "fetch [{}] loaded [{}] rows from [{}] shards in [{}]",
                request.sessionId(),
                response.rows(),
                request.shards().size(),
                TimeValue.timeValueNanos(response.tookNanos())
            );
            try {
                ActionListener.respondAndRelease(listener, response);
            } finally {
                release();
            }
        }

        /**
         * The counters of the driver, and its profile when the query is profiled.
         */
        private DriverCompletionInfo completionInfo(Driver driver) {
            if (request.configuration().profile()) {
                return DriverCompletionInfo.includingProfiles(
                    List.of(driver),
                    DESCRIPTION,
                    clusterService.getClusterName().value(),
                    clusterService.getNodeName(),
                    request.attributes().toString(),
                    null,
                    new PlanTimeProfile(),
                    0L,
                    false,
                    null,
                    null
                );
            }
            return DriverCompletionInfo.excludingProfiles(List.of(driver), 0L, false);
        }

        private void fail(Exception e) {
            try {
                listener.onFailure(e);
            } finally {
                release();
            }
        }

        /**
         * Frees the contexts the request reads for the last time, then drops the request's references on its shards, on the
         * pool that frees search contexts. Once, whichever way the request ended. Only the shards the request opened free
         * their contexts, so a request frees only contexts it was allowed to read.
         */
        private void release() {
            if (released.compareAndSet(false, true) == false) {
                return;
            }
            searchService.getFreeContextExecutor().execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    for (int position = 0; position < contexts.size(); position++) {
                        FetchRequest.ShardDocs shard = request.shards().get(position);
                        if (shard.freeAfterFetch()) {
                            // the request's reference keeps the reader open until it drops it below
                            boolean found = searchService.freeReaderContext(shard.contextId());
                            logger.debug(
                                "freed fetch context [{}] after its last fetch{}",
                                shard.contextId(),
                                found ? "" : ", it was gone"
                            );
                        }
                    }
                    Releasables.close(contexts.stream().map(context -> (Releasable) context::decRef).toList());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to release the shards of a fetch request", e);
                }
            });
        }
    }

    /**
     * The fields among {@code attributes} that some shards may not map. Each shard resolves them to what it holds, like in
     * the query phase, so a fetched column has the values the query phase would have loaded.
     */
    private static Set<String> unmappedFieldNames(List<Attribute> attributes) {
        Set<String> names = new HashSet<>();
        for (Attribute attribute : attributes) {
            if (attribute instanceof FieldAttribute field) {
                PlannerUtils.collectUnmappedFieldName(field, names);
            }
        }
        return names;
    }
}
