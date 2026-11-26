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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.lucene.read.FetchSliceQueue;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverExecutor;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.PlanTimeProfile;
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
import org.elasticsearch.search.scheduling.QueryLoad;
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
 * Then it cuts the documents into slices of a {@link FetchSliceQueue}, and at most {@code task_concurrency} drivers take
 * the slices as they go, like the drivers of the query phase take theirs. So the node loads its shards, and the segments
 * of one shard, in parallel. Without the pragma, the processors of this node set that number. A request starts no more
 * drivers than it has full slices of documents, so a small fetch runs on one driver. The response holds the rows in
 * request order. A request that fails for one shard fails as a whole, like a fetch request of {@code _search}.
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
     * A jumbo size no page reaches, so {@link ValuesSourceReaderOperator} loads each slice as one page.
     */
    private static final ByteSizeValue NO_JUMBO_SIZE = ByteSizeValue.ofBytes(Long.MAX_VALUE);

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
            // the rows of each slice, in slice order. Only the driver that took a slice adds to its list, and the response
            // reads them once every driver finished.
            List<List<Page>> pagesBySlice;
            List<Driver> drivers;
            try {
                task.ensureNotCancelled();
                ensureTheIndicesHoldEveryShard();
                Set<String> unmappedFieldNames = unmappedFieldNames(request.attributes());
                List<FetchDocsSourceOperator.ShardDocs> docs = new ArrayList<>(request.shards().size());
                for (int position = 0; position < request.shards().size(); position++) {
                    FetchRequest.ShardDocs shard = request.shards().get(position);
                    contexts.add(openShard(position, shard, unmappedFieldNames));
                    docs.add(new FetchDocsSourceOperator.ShardDocs(position, shard.segments(), shard.docs()));
                }
                FetchSliceQueue slices = FetchSliceQueue.create(docs);
                pagesBySlice = new ArrayList<>(slices.sliceCount());
                for (int s = 0; s < slices.sliceCount(); s++) {
                    pagesBySlice.add(new ArrayList<>(1));
                }
                drivers = createDrivers(slices, pagesBySlice);
            } catch (Exception e) {
                fail(e);
                return;
            }
            long setupNanos = System.nanoTime() - startNanos;
            // runs at once when the task is already cancelled, and the drivers fail on their first iteration
            task.addListener(() -> {
                String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
                drivers.forEach(driver -> driver.cancel(reason));
            });
            ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
            // a driver that fails cancels the others, and the request fails once all of them ended
            new DriverRunner(threadContext) {
                @Override
                protected void start(Driver driver, ActionListener<Void> driverListener) {
                    Driver.start(threadContext, driverExecutor, driver, Driver.DEFAULT_MAX_ITERATIONS, driverListener);
                }
            }.runToCompletion(drivers, new ActionListener<>() {
                // not ActionListener#wrap: when respond throws, wrap would fail the request again and release the pages twice
                @Override
                public void onResponse(Void ignored) {
                    respond(concat(pagesBySlice), drivers, setupNanos);
                }

                @Override
                public void onFailure(Exception e) {
                    Releasables.closeExpectNoException(concat(pagesBySlice));
                    fail(e);
                }
            });
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
         * The drivers that load the documents. They share {@code slices} and take them as they go, at most
         * {@code task_concurrency} drivers, and never more drivers than full slices of documents.
         */
        private List<Driver> createDrivers(FetchSliceQueue slices, List<List<Page>> pagesBySlice) {
            IndexedByShardIdFromList<DefaultShardContext> shardContexts = new IndexedByShardIdFromList<>(contexts);
            EsPhysicalOperationProviders operationProviders = new EsPhysicalOperationProviders(
                request.configuration().newFoldContext(),
                request.configuration(),
                shardContexts,
                searchService.getIndicesService().getAnalysis(),
                // a slice already bounds its documents, so the reader never cuts its page, and each slice is one page
                plannerSettings.get().valuesLoadingJumboSize(NO_JUMBO_SIZE),
                searchService.getIndicesService()::currentStoreBytesRead,
                QueryWarnings.EMIT
            );
            ValuesSourceReaderOperator.Factory reader = operationProviders.valuesSourceReader(request.attributes(), 0);
            int taskConcurrency = request.configuration().pragmas().taskConcurrency(clusterService.getSettings());
            // A driver gets at least about one full slice of documents, also when the documents lie in many small slices.
            // A slice holds at most SLICE_DOCS documents, so this never starts more drivers than slices.
            int fullSlices = Math.ceilDiv(slices.docCount(), FetchSliceQueue.SLICE_DOCS);
            int driverCount = Math.max(1, Math.min(taskConcurrency, fullSlices));
            List<Driver> drivers = new ArrayList<>(driverCount);
            boolean success = false;
            try {
                for (int d = 0; d < driverCount; d++) {
                    drivers.add(createDriver(shardContexts, reader, slices, pagesBySlice));
                }
                success = true;
                return drivers;
            } finally {
                if (success == false) {
                    // no driver started, so nothing else closes them
                    Releasables.closeExpectNoException(drivers);
                }
            }
        }

        /**
         * A driver that takes slices from {@code slices} and loads the attributes of the request for their documents. It
         * adds the rows of each slice, without the {@code _doc} column and the position of the slice, to the list of that
         * slice in {@code pagesBySlice}.
         */
        private Driver createDriver(
            IndexedByShardIdFromList<DefaultShardContext> shardContexts,
            ValuesSourceReaderOperator.Factory readerFactory,
            FetchSliceQueue slices,
            List<List<Page>> pagesBySlice
        ) {
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
                FetchDocsSourceOperator source = new FetchDocsSourceOperator(driverContext.blockFactory(), shardContexts, slices);
                built.add(source);
                Operator reader = readerFactory.get(driverContext);
                built.add(reader);
                // the source emits the _doc column and the position of the slice, and the reader adds the attributes
                int[] attributes = IntStream.rangeClosed(2, request.attributes().size() + 1).toArray();
                OutputOperator output = new OutputOperator(List.of(), Function.identity(), page -> {
                    try {
                        int slice = page.<IntBlock>getBlock(1).getInt(0);
                        Page rows = page.projectBlocks(attributes);
                        // the response releases the rows on another thread
                        rows.allowPassingToDifferentDriver();
                        pagesBySlice.get(slice).add(rows);
                    } finally {
                        // releases the _doc column and with it the references of the page on its shard
                        page.releaseBlocks();
                    }
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
                    List.of(reader),
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

        private void respond(List<Page> pages, List<Driver> drivers, long setupNanos) {
            FetchResponse response;
            try {
                response = new FetchResponse(pages, completionInfo(drivers), System.nanoTime() - startNanos, setupNanos);
            } catch (Exception e) {
                Releasables.closeExpectNoException(pages);
                fail(e);
                return;
            }
            // the coordinator places the rows of the response by their position, so every document needs one row
            assert response.rows() == request.totalDocs()
                : "fetched [" + response.rows() + "] rows for [" + request.totalDocs() + "] documents";
            logger.debug(
                "fetch [{}] loaded [{}] rows from [{}] shards on [{}] drivers in [{}]",
                request.sessionId(),
                response.rows(),
                request.shards().size(),
                drivers.size(),
                TimeValue.timeValueNanos(response.tookNanos())
            );
            try {
                ActionListener.respondAndRelease(listener, response);
            } finally {
                release();
            }
        }

        /**
         * The counters of the drivers, and their profiles when the query is profiled.
         */
        private DriverCompletionInfo completionInfo(List<Driver> drivers) {
            if (request.configuration().profile()) {
                return DriverCompletionInfo.includingProfiles(
                    drivers,
                    DESCRIPTION,
                    clusterService.getClusterName().value(),
                    clusterService.getNodeName(),
                    request.attributes().toString(),
                    null,
                    new PlanTimeProfile(),
                    0L,
                    new QueryLoad(),
                    false,
                    null,
                    null
                );
            }
            return DriverCompletionInfo.excludingProfiles(drivers, 0L, new QueryLoad(), false);
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

    private static List<Page> concat(List<List<Page>> pagesBySlice) {
        List<Page> pages = new ArrayList<>();
        for (List<Page> slicePages : pagesBySlice) {
            pages.addAll(slicePages);
        }
        return pages;
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
