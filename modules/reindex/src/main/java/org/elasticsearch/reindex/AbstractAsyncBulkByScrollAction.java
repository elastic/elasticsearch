/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ClientScrollableHitSource;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Metadata;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all results. All package private methods are package private so
 * their tests can use them. Most methods run in the listener thread pool because they are meant to be fast and don't expect to block.
 */
public abstract class AbstractAsyncBulkByScrollAction<
    Request extends AbstractBulkByScrollRequest<Request>,
    Action extends TransportAction<Request, ?>> {

    protected final Logger logger;
    protected final BulkByScrollTask task;
    protected final WorkerBulkByScrollTaskState worker;
    protected final ThreadPool threadPool;
    protected final ScriptService scriptService;
    protected final ReindexSslConfig sslConfig;

    /**
     * The request for this action. Named mainRequest because we create lots of <code>request</code> variables all representing child
     * requests of this mainRequest.
     */
    protected final Request mainRequest;

    private final AtomicLong startTime = new AtomicLong(-1);
    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ParentTaskAssigningClient searchClient;
    private final ParentTaskAssigningClient bulkClient;
    private final ActionListener<BulkByScrollResponse> listener;
    private final Retry bulkRetry;
    private final ScrollableHitSource scrollSource;

    /**
     * This BiFunction is used to apply various changes depending of the Reindex action and  the search hit,
     * from copying search hit metadata (parent, routing, etc) to potentially transforming the
     * {@link RequestWrapper} completely.
     */
    private final BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> scriptApplier;
    private int lastBatchSize;
    /**
     * Keeps track of the total number of bulk operations performed
     * from a single scroll response. It is possible that
     * multiple bulk requests are performed from a single scroll
     * response, meaning that we have to take into account the total
     * in order to compute a correct scroll keep alive time.
     */
    private final AtomicInteger totalBatchSizeInSingleScrollResponse = new AtomicInteger();

    AbstractAsyncBulkByScrollAction(
        BulkByScrollTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        Logger logger,
        ParentTaskAssigningClient client,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByScrollResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig
    ) {
        this(
            task,
            needsSourceDocumentVersions,
            needsSourceDocumentSeqNoAndPrimaryTerm,
            logger,
            client,
            client,
            threadPool,
            mainRequest,
            listener,
            scriptService,
            sslConfig
        );
    }

    AbstractAsyncBulkByScrollAction(
        BulkByScrollTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        Logger logger,
        ParentTaskAssigningClient searchClient,
        ParentTaskAssigningClient bulkClient,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByScrollResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig
    ) {
        this.task = task;
        this.scriptService = scriptService;
        this.sslConfig = sslConfig;
        if (task.isWorker() == false) {
            throw new IllegalArgumentException("Given task [" + task.getId() + "] must have a child worker");
        }
        this.worker = task.getWorkerState();

        this.logger = logger;
        this.searchClient = searchClient;
        this.bulkClient = bulkClient;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = new Retry(BackoffPolicy.wrap(backoffPolicy, worker::countBulkRetry), threadPool);
        scrollSource = buildScrollableResultSource(
            backoffPolicy,
            prepareSearchRequest(mainRequest, needsSourceDocumentVersions, needsSourceDocumentSeqNoAndPrimaryTerm)
        );
        scriptApplier = Objects.requireNonNull(buildScriptApplier(), "script applier must not be null");
    }

    /**
     * Prepares a search request to be used in a ScrollableHitSource.
     * Preparation might set a sort order (if not set already) and disable scroll if max docs is small enough.
     */
    // Visible for testing
    static <Request extends AbstractBulkByScrollRequest<Request>> SearchRequest prepareSearchRequest(
        Request mainRequest,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm
    ) {
        var preparedSearchRequest = new SearchRequest(mainRequest.getSearchRequest());

        /*
         * Default to sorting by doc. We can't do this in the request itself because it is normal to *add* to the sorts rather than replace
         * them and if we add _doc as the first sort by default then sorts will never work.... So we add it here, only if there isn't
         * another sort.
         *
         * This modifies the original request!
         */
        final SearchSourceBuilder sourceBuilder = preparedSearchRequest.source();
        List<SortBuilder<?>> sorts = sourceBuilder.sorts();
        if (sorts == null || sorts.isEmpty()) {
            sourceBuilder.sort(fieldSort("_doc"));
        }
        sourceBuilder.version(needsSourceDocumentVersions);
        sourceBuilder.seqNoAndPrimaryTerm(needsSourceDocumentSeqNoAndPrimaryTerm);

        /*
         * Do not open scroll if max docs <= scroll size and not resuming on version conflicts
         */
        if (mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES
            && mainRequest.getMaxDocs() <= preparedSearchRequest.source().size()
            && mainRequest.isAbortOnVersionConflict()) {
            preparedSearchRequest.scroll((Scroll) null);
        }

        return preparedSearchRequest;
    }

    /**
     * Build the {@link BiFunction} to apply to all {@link RequestWrapper}.
     *
     * Public for testings....
     */
    public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
        // The default script applier executes a no-op
        return (request, searchHit) -> request;
    }

    /**
     * Build the {@link RequestWrapper} for a single search hit. This shouldn't handle
     * metadata or scripting. That will be handled by copyMetadata and
     * apply functions that can be overridden.
     */
    protected abstract RequestWrapper<?> buildRequest(ScrollableHitSource.Hit doc);

    /**
     * Copies the metadata from a hit to the request.
     */
    protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
        copyRouting(request, doc.getRouting());
        return request;
    }

    /**
     * Copy the routing from a search hit to the request.
     */
    protected void copyRouting(RequestWrapper<?> request, String routing) {
        request.setRouting(routing);
    }

    /**
     * Used to accept or ignore a search hit. Ignored search hits will be excluded
     * from the bulk request. It is also where we fail on invalid search hits, like
     * when the document has no source but it's required.
     */
    protected boolean accept(ScrollableHitSource.Hit doc) {
        if (doc.getSource() == null) {
            /*
             * Either the document didn't store _source or we didn't fetch it for some reason. Since we don't allow the user to
             * change the "fields" part of the search request it is unlikely that we got here because we didn't fetch _source.
             * Thus the error message assumes that it wasn't stored.
             */
            throw new IllegalArgumentException("[" + doc.getIndex() + "][" + doc.getId() + "] didn't store _source");
        }
        return true;
    }

    protected BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (ScrollableHitSource.Hit doc : docs) {
            if (accept(doc)) {
                RequestWrapper<?> request = scriptApplier.apply(copyMetadata(buildRequest(doc), doc), doc);
                if (request != null) {
                    bulkRequest.add(request.self());
                }
            }
        }
        return bulkRequest;
    }

    protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy, SearchRequest searchRequest) {
        return new ClientScrollableHitSource(
            logger,
            backoffPolicy,
            threadPool,
            worker::countSearchRetry,
            this::onScrollResponse,
            this::finishHim,
            searchClient,
            searchRequest
        );
    }

    /**
     * Build the response for reindex actions.
     */
    protected BulkByScrollResponse buildResponse(
        TimeValue took,
        List<BulkItemResponse.Failure> indexingFailures,
        List<SearchFailure> searchFailures,
        boolean timedOut
    ) {
        return new BulkByScrollResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut);
    }

    /**
     * Start the action by firing the initial search request.
     */
    public void start() {
        logger.debug("[{}]: starting", task.getId());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        try {
            startTime.set(System.nanoTime());
            scrollSource.start();
        } catch (Exception e) {
            finishHim(e);
        }
    }

    void onScrollResponse(ScrollableHitSource.AsyncResponse asyncResponse) {
        onScrollResponse(new ScrollConsumableHitsResponse(asyncResponse));
    }

    void onScrollResponse(ScrollConsumableHitsResponse asyncResponse) {
        // lastBatchStartTime is essentially unused (see WorkerBulkByScrollTaskState.throttleWaitTime. Leaving it for now, since it seems
        // like a bug?
        onScrollResponse(System.nanoTime(), this.lastBatchSize, asyncResponse);
    }

    /**
     * Process a scroll response.
     * @param lastBatchStartTimeNS the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSizeToUse the size of the last batch. Used to calculate the throttling delay.
     * @param asyncResponse the response to process from ScrollableHitSource
     */
    void onScrollResponse(long lastBatchStartTimeNS, int lastBatchSizeToUse, ScrollConsumableHitsResponse asyncResponse) {
        ScrollableHitSource.Response response = asyncResponse.response();
        logger.debug("[{}]: got scroll response with [{}] hits", task.getId(), asyncResponse.remainingHits());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        if (    // If any of the shards failed that should abort the request.
        (response.getFailures().size() > 0)
            // Timeouts aren't shard failures but we still need to pass them back to the user.
            || response.isTimedOut()) {
            refreshAndFinish(emptyList(), response.getFailures(), response.isTimedOut());
            return;
        }
        long total = response.getTotalHits();
        if (mainRequest.getMaxDocs() > 0) {
            total = min(total, mainRequest.getMaxDocs());
        }
        worker.setTotal(total);
        AbstractRunnable prepareBulkRequestRunnable = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                /*
                 * It is important that the batch start time be calculated from here, scroll response to scroll response. That way the time
                 * waiting on the scroll doesn't count against this batch in the throttle.
                 */
                prepareBulkRequest(System.nanoTime(), asyncResponse);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        };
        prepareBulkRequestRunnable = (AbstractRunnable) threadPool.getThreadContext().preserveContext(prepareBulkRequestRunnable);
        worker.delayPrepareBulkRequest(threadPool, lastBatchStartTimeNS, lastBatchSizeToUse, prepareBulkRequestRunnable);
    }

    /**
     * Prepare the bulk request. Called on the generic thread pool after some preflight checks have been done one the SearchResponse and any
     * delay has been slept. Uses the generic thread pool because reindex is rare enough not to need its own thread pool and because the
     * thread may be blocked by the user script.
     */
    void prepareBulkRequest(long thisBatchStartTimeNS, ScrollConsumableHitsResponse asyncResponse) {
        logger.debug("[{}]: preparing bulk request", task.getId());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        if (asyncResponse.hasRemainingHits() == false) {
            refreshAndFinish(emptyList(), emptyList(), false);
            return;
        }
        worker.countBatch();
        final List<? extends ScrollableHitSource.Hit> hits;

        if (mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES) {
            // Truncate the hits if we have more than the request max docs
            long remainingDocsToProcess = max(0, mainRequest.getMaxDocs() - worker.getSuccessfullyProcessed());
            hits = remainingDocsToProcess < asyncResponse.remainingHits()
                ? asyncResponse.consumeHits((int) remainingDocsToProcess)
                : asyncResponse.consumeRemainingHits();
        } else {
            hits = asyncResponse.consumeRemainingHits();
        }

        BulkRequest request = buildBulk(hits);
        if (request.requests().isEmpty()) {
            /*
             * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
             */
            notifyDone(thisBatchStartTimeNS, asyncResponse, 0);
            return;
        }
        request.timeout(mainRequest.getTimeout());
        request.waitForActiveShards(mainRequest.getWaitForActiveShards());
        sendBulkRequest(request, () -> notifyDone(thisBatchStartTimeNS, asyncResponse, request.requests().size()));
    }

    /**
     * Send a bulk request, handling retries.
     */
    void sendBulkRequest(BulkRequest request, Runnable onSuccess) {
        final int requestSize = request.requests().size();
        if (logger.isDebugEnabled()) {
            logger.debug(
                "[{}]: sending [{}] entry, [{}] bulk request",
                task.getId(),
                requestSize,
                new ByteSizeValue(request.estimatedSizeInBytes())
            );
        }
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        bulkRetry.withBackoff(bulkClient::bulk, request, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                logger.debug("[{}]: completed [{}] entry bulk request", task.getId(), requestSize);
                onBulkResponse(response, onSuccess);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    /**
     * Processes bulk responses, accounting for failures.
     */
    void onBulkResponse(BulkResponse response, Runnable onSuccess) {
        try {
            List<Failure> failures = new ArrayList<>();
            Set<String> destinationIndicesThisBatch = new HashSet<>();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    recordFailure(item.getFailure(), failures);
                    continue;
                }
                switch (item.getOpType()) {
                    case CREATE:
                    case INDEX:
                        if (item.getResponse().getResult() == DocWriteResponse.Result.CREATED) {
                            worker.countCreated();
                        } else {
                            worker.countUpdated();
                        }
                        break;
                    case UPDATE:
                        worker.countUpdated();
                        break;
                    case DELETE:
                        worker.countDeleted();
                        break;
                }
                // Track the indexes we've seen so we can refresh them if requested
                destinationIndicesThisBatch.add(item.getIndex());
            }

            if (task.isCancelled()) {
                logger.debug("[{}]: Finishing early because the task was cancelled", task.getId());
                finishHim(null);
                return;
            }

            addDestinationIndices(destinationIndicesThisBatch);

            if (false == failures.isEmpty()) {
                refreshAndFinish(unmodifiableList(failures), emptyList(), false);
                return;
            }

            if (mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES && worker.getSuccessfullyProcessed() >= mainRequest.getMaxDocs()) {
                // We've processed all the requested docs.
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            if (scrollSource.hasScroll() == false) {
                // Index contains fewer matching docs than max_docs (found < max_docs <= scroll size)
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            onSuccess.run();
        } catch (Exception t) {
            finishHim(t);
        }
    }

    void notifyDone(long thisBatchStartTimeNS, ScrollConsumableHitsResponse asyncResponse, int batchSize) {
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        this.lastBatchSize = batchSize;
        this.totalBatchSizeInSingleScrollResponse.addAndGet(batchSize);

        if (asyncResponse.hasRemainingHits() == false) {
            int totalBatchSize = totalBatchSizeInSingleScrollResponse.getAndSet(0);
            asyncResponse.done(worker.throttleWaitTime(thisBatchStartTimeNS, System.nanoTime(), totalBatchSize));
        } else {
            onScrollResponse(asyncResponse);
        }
    }

    private void recordFailure(Failure failure, List<Failure> failures) {
        if (failure.getStatus() == CONFLICT) {
            worker.countVersionConflict();
            if (false == mainRequest.isAbortOnVersionConflict()) {
                return;
            }
        }
        failures.add(failure);
    }

    /**
     * Start terminating a request that finished non-catastrophically by refreshing the modified indices and then proceeding to
     * {@link #finishHim(Exception, List, List, boolean)}.
     */
    void refreshAndFinish(List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        if (task.isCancelled() || false == mainRequest.isRefresh() || destinationIndices.isEmpty()) {
            finishHim(null, indexingFailures, searchFailures, timedOut);
            return;
        }
        RefreshRequest refresh = new RefreshRequest();
        refresh.indices(destinationIndices.toArray(new String[destinationIndices.size()]));
        logger.debug("[{}]: refreshing", task.getId());
        bulkClient.admin().indices().refresh(refresh, new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse response) {
                finishHim(null, indexingFailures, searchFailures, timedOut);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    /**
     * Finish the request.
     *
     * @param failure if non null then the request failed catastrophically with this exception
     */
    protected void finishHim(Exception failure) {
        logger.debug(() -> "[" + task.getId() + "]: finishing with a catastrophic failure", failure);
        finishHim(failure, emptyList(), emptyList(), false);
    }

    /**
     * Finish the request.
     * @param failure if non null then the request failed catastrophically with this exception
     * @param indexingFailures any indexing failures accumulated during the request
     * @param searchFailures any search failures accumulated during the request
     * @param timedOut have any of the sub-requests timed out?
     */
    protected void finishHim(Exception failure, List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        logger.debug("[{}]: finishing without any catastrophic failures", task.getId());
        scrollSource.close(threadPool.getThreadContext().preserveContext(() -> {
            if (failure == null) {
                BulkByScrollResponse response = buildResponse(
                    timeValueNanos(System.nanoTime() - startTime.get()),
                    indexingFailures,
                    searchFailures,
                    timedOut
                );
                listener.onResponse(response);
            } else {
                listener.onFailure(failure);
            }
        }));
    }

    /**
     * Get the backoff policy for use with retries.
     */
    BackoffPolicy buildBackoffPolicy() {
        return exponentialBackoff(mainRequest.getRetryBackoffInitialTime(), mainRequest.getMaxRetries());
    }

    /**
     * Add to the list of indices that were modified by this request. This is the list of indices refreshed at the end of the request if the
     * request asks for a refresh.
     */
    void addDestinationIndices(Collection<String> indices) {
        destinationIndices.addAll(indices);
    }

    /**
     * Set the last returned scrollId. Exists entirely for testing.
     */
    void setScroll(String scroll) {
        scrollSource.setScroll(scroll);
    }

    /**
     * Wrapper for the {@link DocWriteRequest} that are used in this action class.
     */
    public interface RequestWrapper<Self extends DocWriteRequest<Self>> {

        void setIndex(String index);

        String getIndex();

        void setId(String id);

        String getId();

        void setVersion(long version);

        long getVersion();

        void setVersionType(VersionType versionType);

        void setRouting(String routing);

        String getRouting();

        void setSource(Map<String, Object> source);

        Map<String, Object> getSource();

        Self self();
    }

    /**
     * {@link RequestWrapper} for {@link IndexRequest}
     */
    public static class IndexRequestWrapper implements RequestWrapper<IndexRequest> {

        private final IndexRequest request;

        IndexRequestWrapper(IndexRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped IndexRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public Map<String, Object> getSource() {
            return request.sourceAsMap();
        }

        @Override
        public void setSource(Map<String, Object> source) {
            request.source(source);
        }

        @Override
        public IndexRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link IndexRequest} in a {@link RequestWrapper}
     */
    public static RequestWrapper<IndexRequest> wrap(IndexRequest request) {
        return new IndexRequestWrapper(request);
    }

    /**
     * {@link RequestWrapper} for {@link DeleteRequest}
     */
    public static class DeleteRequestWrapper implements RequestWrapper<DeleteRequest> {

        private final DeleteRequest request;

        DeleteRequestWrapper(DeleteRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped DeleteRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public Map<String, Object> getSource() {
            throw new UnsupportedOperationException("unable to get source from action request [" + request.getClass() + "]");
        }

        @Override
        public void setSource(Map<String, Object> source) {
            throw new UnsupportedOperationException("unable to set [source] on action request [" + request.getClass() + "]");
        }

        @Override
        public DeleteRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link DeleteRequest} in a {@link RequestWrapper}
     */
    public static RequestWrapper<DeleteRequest> wrap(DeleteRequest request) {
        return new DeleteRequestWrapper(request);
    }

    /**
     * Apply a {@link Script} to a {@link RequestWrapper}
     */
    public abstract static class ScriptApplier<T extends Metadata>
        implements
            BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> {

        // "index" is the default operation
        protected static final String INDEX = "index";

        private final WorkerBulkByScrollTaskState taskWorker;
        protected final ScriptService scriptService;
        protected final Script script;
        protected final Map<String, Object> params;
        protected final LongSupplier nowInMillisSupplier;

        public ScriptApplier(
            WorkerBulkByScrollTaskState taskWorker,
            ScriptService scriptService,
            Script script,
            Map<String, Object> params,
            LongSupplier nowInMillisSupplier
        ) {
            this.taskWorker = taskWorker;
            this.scriptService = scriptService;
            this.script = script;
            this.params = params;
            this.nowInMillisSupplier = nowInMillisSupplier;
        }

        @Override
        public RequestWrapper<?> apply(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
            if (script == null) {
                return request;
            }

            CtxMap<T> ctxMap = execute(doc, request.getSource());

            T metadata = ctxMap.getMetadata();

            request.setSource(ctxMap.getSource());

            updateRequest(request, metadata);

            return requestFromOp(request, metadata.getOp());
        }

        protected abstract CtxMap<T> execute(ScrollableHitSource.Hit doc, Map<String, Object> source);

        protected abstract void updateRequest(RequestWrapper<?> request, T metadata);

        protected RequestWrapper<?> requestFromOp(RequestWrapper<?> request, String op) {
            switch (op) {
                case "noop" -> {
                    taskWorker.countNoop();
                    return null;
                }
                case "delete" -> {
                    RequestWrapper<DeleteRequest> delete = wrap(new DeleteRequest(request.getIndex(), request.getId()));
                    delete.setVersion(request.getVersion());
                    delete.setVersionType(VersionType.INTERNAL);
                    delete.setRouting(request.getRouting());
                    return delete;
                }
                case INDEX -> {
                    return request;
                }
                default -> throw new IllegalArgumentException("Unsupported operation type change from [" + INDEX + "] to [" + op + "]");
            }
        }
    }

    static class ScrollConsumableHitsResponse {
        private final ScrollableHitSource.AsyncResponse asyncResponse;
        private final List<? extends ScrollableHitSource.Hit> hits;
        private int consumedOffset = 0;

        ScrollConsumableHitsResponse(ScrollableHitSource.AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
            this.hits = asyncResponse.response().getHits();
        }

        ScrollableHitSource.Response response() {
            return asyncResponse.response();
        }

        List<? extends ScrollableHitSource.Hit> consumeRemainingHits() {
            return consumeHits(remainingHits());
        }

        List<? extends ScrollableHitSource.Hit> consumeHits(int numberOfHits) {
            if (numberOfHits < 0) {
                throw new IllegalArgumentException("Invalid number of hits to consume [" + numberOfHits + "]");
            }

            if (numberOfHits > remainingHits()) {
                throw new IllegalArgumentException(
                    "Unable to provide [" + numberOfHits + "] hits as there are only [" + remainingHits() + "] hits available"
                );
            }

            int start = consumedOffset;
            consumedOffset += numberOfHits;
            return hits.subList(start, consumedOffset);
        }

        boolean hasRemainingHits() {
            return remainingHits() > 0;
        }

        int remainingHits() {
            return hits.size() - consumedOffset;
        }

        void done(TimeValue extraKeepAlive) {
            asyncResponse.done(extraKeepAlive);
        }
    }
}
