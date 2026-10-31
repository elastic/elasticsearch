/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.fetch.FetchResponse.ShardResult;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Runs fetch requests on the node that holds their documents.
 * <p>
 * For each shard it finds the reader context the query phase kept open and builds a search context on it, on the thread
 * that received the request, so the searcher applies the document and field level security of that request. Then it
 * plans the fetch plan once and runs it in one driver per piece of a shard, at most
 * {@link FetchService#MAX_CONCURRENT_SHARD_TASKS} at a time. A shard with enough documents splits into as many pieces, so
 * a node whose documents sit in one shard still loads them on several threads. A shard that fails doesn't stop the others,
 * and a piece that fails fails its shard. The response returns the rows of each shard in request order.
 * <p>
 * The executor holds the first reference on the context of each shard, and the pages take their own while they load. It
 * drops its references once the response is sent, on the generic pool, because dropping the last reference to a reader
 * can block.
 */
final class DataNodeFetchExecutor {
    private static final Logger logger = LogManager.getLogger(DataNodeFetchExecutor.class);

    static final String DESCRIPTION = ComputeService.FETCH_DESCRIPTION;

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final SearchService searchService;
    private final BlockFactory blockFactory;
    private final Executor driverExecutor;
    private final Supplier<PlannerSettings> plannerSettings;
    private final FetchPlannerFactory plannerFactory;
    private final IntSupplier maxConcurrentShardTasks;

    DataNodeFetchExecutor(
        TransportService transportService,
        ClusterService clusterService,
        SearchService searchService,
        BlockFactory blockFactory,
        Executor driverExecutor,
        Supplier<PlannerSettings> plannerSettings,
        FetchPlannerFactory plannerFactory,
        IntSupplier maxConcurrentShardTasks
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.blockFactory = blockFactory;
        this.driverExecutor = driverExecutor;
        this.plannerSettings = plannerSettings;
        this.plannerFactory = plannerFactory;
        this.maxConcurrentShardTasks = maxConcurrentShardTasks;
    }

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
         * The result of each shard of the request, set when the shard fails to open or once its driver is done.
         */
        private final ShardResult[] results;
        /**
         * The context of each shard of the request, {@code null} for a shard that failed to open.
         */
        private final List<DefaultShardContext> contexts;
        /**
         * Where the pieces of each shard start in the pieces of the request, with one more entry for the end. A shard that
         * failed to open has no pieces. Set before the drivers start.
         */
        private int[] firstPieceOfShard;
        /**
         * The failure of each piece, set by its driver.
         */
        private AtomicReferenceArray<Exception> pieceFailures;
        @Nullable
        private ShardPages shardPages;
        private List<PieceDriver> pieceDrivers = List.of();
        private long setupNanos;
        private final AtomicBoolean released = new AtomicBoolean();

        Execution(FetchRequest request, CancellableTask task, ActionListener<FetchResponse> listener) {
            this.request = request;
            this.task = task;
            this.listener = listener;
            int shards = request.shards().size();
            this.results = new ShardResult[shards];
            this.contexts = new ArrayList<>(Arrays.asList(new DefaultShardContext[shards]));
        }

        void start() {
            try {
                task.ensureNotCancelled();
                checkFetchPlan(request.fetchPlan().output());
                List<FetchDocsSourceOperator.ShardDocs> open = openShards();
                if (open.isEmpty() == false) {
                    // the last driver to finish answers
                    startDrivers(open);
                    return;
                }
                setupNanos = System.nanoTime() - startNanos;
            } catch (Exception e) {
                fail(e);
                return;
            }
            respond(List.of(), DriverCompletionInfo.EMPTY);
        }

        /**
         * Opens a search context for every shard the request is allowed to read, and records the failure of the others.
         *
         * @return the open shards
         */
        private List<FetchDocsSourceOperator.ShardDocs> openShards() {
            IndicesAccessControl accessControl = ShardAccess.of(transportService.getThreadPool().getThreadContext());
            List<FetchDocsSourceOperator.ShardDocs> open = new ArrayList<>(request.shards().size());
            for (int position = 0; position < request.shards().size(); position++) {
                FetchRequest.ShardDocs shard = request.shards().get(position);
                try {
                    ShardAccess.check(accessControl, shard.shardId(), FetchService.FETCH_ACTION_NAME);
                    contexts.set(position, openShard(position, shard));
                    open.add(new FetchDocsSourceOperator.ShardDocs(position, shard.segments(), shard.docs()));
                } catch (Exception e) {
                    results[position] = ShardResult.failed(shard.shardId(), e);
                }
            }
            return open;
        }

        private DefaultShardContext openShard(int position, FetchRequest.ShardDocs shard) throws Exception {
            ReaderContext readerContext = searchService.findReaderContext(shard.contextId(), request, shard.shardId());
            // the lookup ignores the searcher id, and the documents of another searcher are other documents. A scroll or a
            // point in time whose id the request learned isn't for the fetch phase either, as for the fetch phase of _search.
            if (readerContext.id().equals(shard.contextId()) == false || readerContext.singleSession() == false) {
                throw new SearchContextMissingException(shard.contextId());
            }
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
                checkBounds(searchContext, shard);
                EsqlSearchExecutionContext executionContext = new EsqlSearchExecutionContext(
                    searchContext.getSearchExecutionContext(),
                    QueryWarnings.EMIT
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
         * Plans the fetch plan and starts its drivers, one per piece of an open shard. Once they start, the last one to finish
         * answers the request.
         */
        private void startDrivers(List<FetchDocsSourceOperator.ShardDocs> open) {
            int maxConcurrentDrivers = maxConcurrentShardTasks.getAsInt();
            List<FetchDocsSourceOperator.ShardDocs> pieces = split(open, maxConcurrentDrivers);
            pieceFailures = new AtomicReferenceArray<>(pieces.size());
            FetchShardContexts<DefaultShardContext> shardContexts = new FetchShardContexts<>(contexts);
            FetchShardAssignment assignment = new FetchShardAssignment(pieces, shardContexts);
            FoldContext foldCtx = request.configuration().newFoldContext();
            LocalExecutionPlanner planner = plannerFactory.create(
                request.sessionId(),
                request.clusterAlias(),
                task,
                request.configuration(),
                foldCtx,
                shardContexts,
                assignment
            );
            shardPages = new ShardPages(pieces.size());
            LocalExecutionPlanner.LocalExecutionPlan plan = planner.plan(
                DESCRIPTION,
                foldCtx,
                plannerSettings.get(),
                request.fetchPlan(),
                shardContexts,
                shardPages.sinks(assignment::pieceOf)
            );
            List<Driver> created = plan.createDrivers(new TaskId(clusterService.localNode().getId(), task.getId()).toString());
            List<PieceDriver> byPiece = new ArrayList<>(created.size());
            boolean success = false;
            try {
                for (Driver driver : created) {
                    byPiece.add(new PieceDriver(driver, assignment.pieceOf(driver.driverContext())));
                }
                success = true;
            } finally {
                if (success == false) {
                    // they never started, so nothing else closes them
                    Releasables.close(created);
                }
            }
            pieceDrivers = byPiece;
            setupNanos = System.nanoTime() - startNanos;

            ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
            ThrottledIterator.run(pieceDrivers.iterator(), (releasable, pieceDriver) -> {
                Driver driver = pieceDriver.driver();
                // runs at once when the task is already cancelled, and the driver fails on its first iteration
                task.addListener(() -> driver.cancel(Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled")));
                ActionListener<Void> driverListener = ActionListener.wrap(ignored -> {}, e -> pieceFailures.set(pieceDriver.piece(), e));
                Driver.start(
                    threadContext,
                    driverExecutor,
                    driver,
                    Driver.DEFAULT_MAX_ITERATIONS,
                    ActionListener.runAfter(driverListener, releasable::close)
                );
            }, maxConcurrentDrivers, this::onDriversDone);
        }

        /**
         * Splits each open shard into the pieces its drivers load, at most as many as the request runs drivers at the same
         * time, and records where the pieces of each shard start.
         */
        private List<FetchDocsSourceOperator.ShardDocs> split(List<FetchDocsSourceOperator.ShardDocs> open, int maxPiecesPerShard) {
            int shards = request.shards().size();
            firstPieceOfShard = new int[shards + 1];
            List<FetchDocsSourceOperator.ShardDocs> pieces = new ArrayList<>(open.size());
            Iterator<FetchDocsSourceOperator.ShardDocs> openShards = open.iterator();
            FetchDocsSourceOperator.ShardDocs nextOpen = openShards.hasNext() ? openShards.next() : null;
            for (int position = 0; position < shards; position++) {
                firstPieceOfShard[position] = pieces.size();
                if (nextOpen != null && nextOpen.shard() == position) {
                    pieces.addAll(nextOpen.split(maxPiecesPerShard));
                    nextOpen = openShards.hasNext() ? openShards.next() : null;
                }
            }
            firstPieceOfShard[shards] = pieces.size();
            return pieces;
        }

        /**
         * Runs on the thread of the last driver to finish.
         */
        private void onDriversDone() {
            if (task.isCancelled()) {
                fail(new TaskCancelledException(Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled")));
                return;
            }
            List<Page> pages = new ArrayList<>();
            DriverCompletionInfo completionInfo;
            try {
                for (int position = 0; position < request.shards().size(); position++) {
                    if (results[position] != null) {
                        continue;
                    }
                    FetchRequest.ShardDocs shard = request.shards().get(position);
                    List<Page> loaded = new ArrayList<>();
                    Exception failure = null;
                    for (int piece = firstPieceOfShard[position]; piece < firstPieceOfShard[position + 1]; piece++) {
                        loaded.addAll(shardPages.take(piece));
                        if (failure == null) {
                            failure = pieceFailures.get(piece);
                        }
                    }
                    int rows = 0;
                    for (Page page : loaded) {
                        rows += page.getPositionCount();
                    }
                    if (failure == null && rows != shard.docCount()) {
                        failure = new IllegalStateException(
                            "fetched [" + rows + "] rows for [" + shard.docCount() + "] documents of " + shard.shardId()
                        );
                    }
                    if (failure == null) {
                        results[position] = ShardResult.succeeded(shard.shardId(), rows);
                        pages.addAll(loaded);
                    } else {
                        results[position] = ShardResult.failed(shard.shardId(), failure);
                        FetchResponse.releasePages(loaded);
                    }
                }
                completionInfo = completionInfo();
            } catch (Exception e) {
                FetchResponse.releasePages(pages);
                fail(e);
                return;
            }
            respond(pages, completionInfo);
        }

        private void respond(List<Page> pages, DriverCompletionInfo completionInfo) {
            FetchResponse response;
            try {
                response = new FetchResponse(
                    blockFactory,
                    Arrays.asList(results),
                    pages,
                    completionInfo,
                    System.nanoTime() - startNanos,
                    setupNanos
                );
            } catch (Exception e) {
                FetchResponse.releasePages(pages);
                fail(e);
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "fetch [{}] loaded [{}] rows from [{}] shards, [{}] failed, in [{}]",
                    request.sessionId(),
                    response.rows(),
                    request.shards().size(),
                    Arrays.stream(results).filter(r -> r.failure() != null).count(),
                    TimeValue.timeValueNanos(response.tookNanos())
                );
            }
            try {
                ActionListener.respondAndRelease(listener, response);
            } finally {
                release();
            }
        }

        /**
         * The counters and profiles of the drivers that finished. A failed driver has neither, and its shard fails anyway.
         * The other pieces of that shard did load, so their drivers count.
         */
        private DriverCompletionInfo completionInfo() {
            List<Driver> finished = new ArrayList<>(pieceDrivers.size());
            for (PieceDriver pieceDriver : pieceDrivers) {
                if (pieceFailures.get(pieceDriver.piece()) == null) {
                    finished.add(pieceDriver.driver());
                }
            }
            if (request.configuration().profile()) {
                return DriverCompletionInfo.includingProfiles(
                    finished,
                    DESCRIPTION,
                    clusterService.getClusterName().value(),
                    clusterService.getNodeName(),
                    request.fetchPlan().toString(),
                    null,
                    new PlanTimeProfile(),
                    0L,
                    false
                );
            }
            return DriverCompletionInfo.excludingProfiles(finished, 0L, false);
        }

        private void fail(Exception e) {
            try {
                listener.onFailure(e);
            } finally {
                release();
            }
        }

        /**
         * Drops the request's references on its shards, on the generic pool. Once, whichever way the request ended.
         */
        private void release() {
            if (released.compareAndSet(false, true) == false) {
                return;
            }
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    List<Releasable> references = new ArrayList<>(contexts.size() + 1);
                    references.add(shardPages);
                    for (DefaultShardContext context : contexts) {
                        if (context != null) {
                            references.add(context::decRef);
                        }
                    }
                    Releasables.close(references);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to release the shards of a fetch request", e);
                }
            });
        }
    }

    /**
     * @param piece the position of the piece the driver loads in the pieces of its request
     */
    private record PieceDriver(Driver driver, int piece) {}

    /**
     * A document outside the reader would fail deep in the loaders, or load another document.
     */
    private static void checkBounds(SearchContext searchContext, FetchRequest.ShardDocs shard) {
        List<LeafReaderContext> leaves = searchContext.searcher().getIndexReader().leaves();
        int[] segments = shard.segments();
        int[] docs = shard.docs();
        for (int i = 0; i < docs.length; i++) {
            if (segments[i] >= leaves.size() || docs[i] >= leaves.get(segments[i]).reader().maxDoc()) {
                throw new IllegalArgumentException(
                    "document [" + segments[i] + ", " + docs[i] + "] isn't in the reader of " + shard.shardId()
                );
            }
        }
    }

    /**
     * Pages with a {@code _doc} column can't leave the node, and their shard references would leak to the coordinator.
     */
    private static void checkFetchPlan(List<Attribute> output) {
        for (Attribute attribute : output) {
            if (attribute.dataType() == DataType.DOC_DATA_TYPE) {
                throw new IllegalArgumentException("a fetch plan can't return [" + attribute + "]");
            }
        }
    }
}
