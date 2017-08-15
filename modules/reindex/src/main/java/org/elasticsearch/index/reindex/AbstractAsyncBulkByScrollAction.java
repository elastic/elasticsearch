/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all results. All package private methods are package private so
 * their tests can use them. Most methods run in the listener thread pool because the are meant to be fast and don't expect to block.
 */
public abstract class AbstractAsyncBulkByScrollAction<Request extends AbstractBulkByScrollRequest<Request>> {
    protected final Logger logger;
    protected final BulkByScrollTask task;
    protected final WorkerBulkByScrollTaskState worker;
    protected final ThreadPool threadPool;
    protected final ScriptService scriptService;
    protected final ClusterState clusterState;

    /**
     * The request for this action. Named mainRequest because we create lots of <code>request</code> variables all representing child
     * requests of this mainRequest.
     */
    protected final Request mainRequest;

    private final AtomicLong startTime = new AtomicLong(-1);
    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ParentTaskAssigningClient client;
    private final ActionListener<BulkByScrollResponse> listener;
    private final Retry bulkRetry;
    private final ScrollableHitSource scrollSource;
    private final Settings settings;

    /**
     * This BiFunction is used to apply various changes depending of the Reindex action and  the search hit,
     * from copying search hit metadata (parent, routing, etc) to potentially transforming the
     * {@link RequestWrapper} completely.
     */
    private final BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> scriptApplier;

    public AbstractAsyncBulkByScrollAction(BulkByScrollTask task, Logger logger, ParentTaskAssigningClient client,
                                           ThreadPool threadPool, Request mainRequest, ScriptService scriptService,
                                           ClusterState clusterState, ActionListener<BulkByScrollResponse> listener) {
        this(task, logger, client, threadPool, mainRequest, scriptService, clusterState, listener, client.settings());
    }

    public AbstractAsyncBulkByScrollAction(BulkByScrollTask task, Logger logger, ParentTaskAssigningClient client,
            ThreadPool threadPool, Request mainRequest, ScriptService scriptService, ClusterState clusterState,
            ActionListener<BulkByScrollResponse> listener, Settings settings) {

        this.task = task;
        if (!task.isWorker()) {
            throw new IllegalArgumentException("Given task [" + task.getId() + "] must have a child worker");
        }
        this.worker = task.getWorkerState();

        this.logger = logger;
        this.client = client;
        this.settings = settings;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.clusterState = clusterState;
        this.mainRequest = mainRequest;
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = new Retry(EsRejectedExecutionException.class, BackoffPolicy.wrap(backoffPolicy, worker::countBulkRetry), threadPool);
        scrollSource = buildScrollableResultSource(backoffPolicy);
        scriptApplier = Objects.requireNonNull(buildScriptApplier(), "script applier must not be null");
        /*
         * Default to sorting by doc. We can't do this in the request itself because it is normal to *add* to the sorts rather than replace
         * them and if we add _doc as the first sort by default then sorts will never work.... So we add it here, only if there isn't
         * another sort.
         */
        List<SortBuilder<?>> sorts = mainRequest.getSearchRequest().source().sorts();
        if (sorts == null || sorts.isEmpty()) {
            mainRequest.getSearchRequest().source().sort(fieldSort("_doc"));
        }
        mainRequest.getSearchRequest().source().version(needsSourceDocumentVersions());
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
     * Does this operation need the versions of the source documents?
     */
    protected abstract boolean needsSourceDocumentVersions();

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
        request.setParent(doc.getParent());
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
            throw new IllegalArgumentException("[" + doc.getIndex() + "][" + doc.getType() + "][" + doc.getId() + "] didn't store _source");
        }
        return true;
    }

    private BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs) {
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

    protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy) {
        return new ClientScrollableHitSource(logger, backoffPolicy, threadPool, worker::countSearchRetry, this::finishHim, client,
                mainRequest.getSearchRequest());
    }

    /**
     * Build the response for reindex actions.
     */
    protected BulkByScrollResponse buildResponse(TimeValue took, List<BulkItemResponse.Failure> indexingFailures,
                                                      List<SearchFailure> searchFailures, boolean timedOut) {
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
            scrollSource.start(response -> onScrollResponse(timeValueNanos(System.nanoTime()), 0, response));
        } catch (Exception e) {
            finishHim(e);
        }
    }

    /**
     * Process a scroll response.
     * @param lastBatchStartTime the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSize the size of the last batch. Used to calculate the throttling delay.
     * @param response the scroll response to process
     */
    void onScrollResponse(TimeValue lastBatchStartTime, int lastBatchSize, ScrollableHitSource.Response response) {
        logger.debug("[{}]: got scroll response with [{}] hits", task.getId(), response.getHits().size());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        if (    // If any of the shards failed that should abort the request.
                (response.getFailures().size() > 0)
                // Timeouts aren't shard failures but we still need to pass them back to the user.
                || response.isTimedOut()
                ) {
            refreshAndFinish(emptyList(), response.getFailures(), response.isTimedOut());
            return;
        }
        long total = response.getTotalHits();
        if (mainRequest.getSize() > 0) {
            total = min(total, mainRequest.getSize());
        }
        worker.setTotal(total);
        AbstractRunnable prepareBulkRequestRunnable = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                /*
                 * It is important that the batch start time be calculated from here, scroll response to scroll response. That way the time
                 * waiting on the scroll doesn't count against this batch in the throttle.
                 */
                prepareBulkRequest(timeValueNanos(System.nanoTime()), response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        };
        prepareBulkRequestRunnable = (AbstractRunnable) threadPool.getThreadContext().preserveContext(prepareBulkRequestRunnable);
        worker.delayPrepareBulkRequest(threadPool, lastBatchStartTime, lastBatchSize, prepareBulkRequestRunnable);
    }

    /**
     * Prepare the bulk request. Called on the generic thread pool after some preflight checks have been done one the SearchResponse and any
     * delay has been slept. Uses the generic thread pool because reindex is rare enough not to need its own thread pool and because the
     * thread may be blocked by the user script.
     */
    void prepareBulkRequest(TimeValue thisBatchStartTime, ScrollableHitSource.Response response) {
        logger.debug("[{}]: preparing bulk request", task.getId());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        if (response.getHits().isEmpty()) {
            refreshAndFinish(emptyList(), emptyList(), false);
            return;
        }
        worker.countBatch();
        List<? extends ScrollableHitSource.Hit> hits = response.getHits();
        if (mainRequest.getSize() != SIZE_ALL_MATCHES) {
            // Truncate the hits if we have more than the request size
            long remaining = max(0, mainRequest.getSize() - worker.getSuccessfullyProcessed());
            if (remaining < hits.size()) {
                hits = hits.subList(0, (int) remaining);
            }
        }
        BulkRequest request = buildBulk(hits);
        if (request.requests().isEmpty()) {
            /*
             * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
             */
            startNextScroll(thisBatchStartTime, timeValueNanos(System.nanoTime()), 0);
            return;
        }
        request.timeout(mainRequest.getTimeout());
        request.waitForActiveShards(mainRequest.getWaitForActiveShards());
        sendBulkRequest(thisBatchStartTime, request);
    }

    /**
     * Send a bulk request, handling retries.
     */
    void sendBulkRequest(TimeValue thisBatchStartTime, BulkRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}]: sending [{}] entry, [{}] bulk request", task.getId(), request.requests().size(),
                    new ByteSizeValue(request.estimatedSizeInBytes()));
        }
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        bulkRetry.withBackoff(client::bulk, request, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                onBulkResponse(thisBatchStartTime, response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        }, settings);
    }

    /**
     * Processes bulk responses, accounting for failures.
     */
    void onBulkResponse(TimeValue thisBatchStartTime, BulkResponse response) {
        try {
            List<Failure> failures = new ArrayList<Failure>();
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

            if (mainRequest.getSize() != SIZE_ALL_MATCHES && worker.getSuccessfullyProcessed() >= mainRequest.getSize()) {
                // We've processed all the requested docs.
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            startNextScroll(thisBatchStartTime, timeValueNanos(System.nanoTime()), response.getItems().length);
        } catch (Exception t) {
            finishHim(t);
        }
    }

    /**
     * Start the next scroll request.
     *
     * @param lastBatchSize the number of requests sent in the last batch. This is used to calculate the throttling values which are applied
     *        when the scroll returns
     */
    void startNextScroll(TimeValue lastBatchStartTime, TimeValue now, int lastBatchSize) {
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        TimeValue extraKeepAlive = worker.throttleWaitTime(lastBatchStartTime, now, lastBatchSize);
        scrollSource.startNextScroll(extraKeepAlive, response -> {
            onScrollResponse(lastBatchStartTime, lastBatchSize, response);
        });
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
        client.admin().indices().refresh(refresh, new ActionListener<RefreshResponse>() {
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
        logger.debug(() -> new ParameterizedMessage("[{}]: finishing with a catastrophic failure", task.getId()), failure);
        finishHim(failure, emptyList(), emptyList(), false);
    }

    /**
     * Finish the request.
     * @param failure if non null then the request failed catastrophically with this exception
     * @param indexingFailures any indexing failures accumulated during the request
     * @param searchFailures any search failures accumulated during the request
     * @param timedOut have any of the sub-requests timed out?
     */
    protected void finishHim(Exception failure, List<Failure> indexingFailures,
            List<SearchFailure> searchFailures, boolean timedOut) {
        logger.debug("[{}]: finishing without any catastrophic failures", task.getId());
        scrollSource.close(() -> {
            if (failure == null) {
                BulkByScrollResponse response = buildResponse(
                        timeValueNanos(System.nanoTime() - startTime.get()),
                        indexingFailures, searchFailures, timedOut);
                listener.onResponse(response);
            } else {
                listener.onFailure(failure);
            }
        });
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

        void setType(String type);

        String getType();

        void setId(String id);

        String getId();

        void setVersion(long version);

        long getVersion();

        void setVersionType(VersionType versionType);

        void setParent(String parent);

        String getParent();

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
        public void setType(String type) {
            request.type(type);
        }

        @Override
        public String getType() {
            return request.type();
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
        public void setParent(String parent) {
            request.parent(parent);
        }

        @Override
        public String getParent() {
            return request.parent();
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
        public void setType(String type) {
            request.type(type);
        }

        @Override
        public String getType() {
            return request.type();
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
        public void setParent(String parent) {
            request.parent(parent);
        }

        @Override
        public String getParent() {
            return request.parent();
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
    public abstract static class ScriptApplier implements BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> {

        private final WorkerBulkByScrollTaskState taskWorker;
        private final ScriptService scriptService;
        private final Script script;
        private final Map<String, Object> params;

        private ExecutableScript executable;
        private Map<String, Object> context;

        public ScriptApplier(WorkerBulkByScrollTaskState taskWorker,
                             ScriptService scriptService,
                             Script script,
                             Map<String, Object> params) {
            this.taskWorker = taskWorker;
            this.scriptService = scriptService;
            this.script = script;
            this.params = params;
        }

        @Override
        @SuppressWarnings("unchecked")
        public RequestWrapper<?> apply(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
            if (script == null) {
                return request;
            }
            if (executable == null) {
                ExecutableScript.Factory factory = scriptService.compile(script, ExecutableScript.UPDATE_CONTEXT);
                executable = factory.newInstance(params);
            }
            if (context == null) {
                context = new HashMap<>();
            } else {
                context.clear();
            }

            context.put(IndexFieldMapper.NAME, doc.getIndex());
            context.put(TypeFieldMapper.NAME, doc.getType());
            context.put(IdFieldMapper.NAME, doc.getId());
            Long oldVersion = doc.getVersion();
            context.put(VersionFieldMapper.NAME, oldVersion);
            String oldParent = doc.getParent();
            context.put(ParentFieldMapper.NAME, oldParent);
            String oldRouting = doc.getRouting();
            context.put(RoutingFieldMapper.NAME, oldRouting);
            context.put(SourceFieldMapper.NAME, request.getSource());

            OpType oldOpType = OpType.INDEX;
            context.put("op", oldOpType.toString());

            executable.setNextVar("ctx", context);
            executable.run();

            String newOp = (String) context.remove("op");
            if (newOp == null) {
                throw new IllegalArgumentException("Script cleared operation type");
            }

            /*
             * It'd be lovely to only set the source if we know its been modified
             * but it isn't worth keeping two copies of it around just to check!
             */
            request.setSource((Map<String, Object>) context.remove(SourceFieldMapper.NAME));

            Object newValue = context.remove(IndexFieldMapper.NAME);
            if (false == doc.getIndex().equals(newValue)) {
                scriptChangedIndex(request, newValue);
            }
            newValue = context.remove(TypeFieldMapper.NAME);
            if (false == doc.getType().equals(newValue)) {
                scriptChangedType(request, newValue);
            }
            newValue = context.remove(IdFieldMapper.NAME);
            if (false == doc.getId().equals(newValue)) {
                scriptChangedId(request, newValue);
            }
            newValue = context.remove(VersionFieldMapper.NAME);
            if (false == Objects.equals(oldVersion, newValue)) {
                scriptChangedVersion(request, newValue);
            }
            newValue = context.remove(ParentFieldMapper.NAME);
            if (false == Objects.equals(oldParent, newValue)) {
                scriptChangedParent(request, newValue);
            }
            /*
             * Its important that routing comes after parent in case you want to
             * change them both.
             */
            newValue = context.remove(RoutingFieldMapper.NAME);
            if (false == Objects.equals(oldRouting, newValue)) {
                scriptChangedRouting(request, newValue);
            }

            OpType newOpType = OpType.fromString(newOp);
            if (newOpType != oldOpType) {
                return scriptChangedOpType(request, oldOpType, newOpType);
            }

            if (false == context.isEmpty()) {
                throw new IllegalArgumentException("Invalid fields added to context [" + String.join(",", context.keySet()) + ']');
            }
            return request;
        }

        protected RequestWrapper<?> scriptChangedOpType(RequestWrapper<?> request, OpType oldOpType, OpType newOpType) {
            switch (newOpType) {
            case NOOP:
                taskWorker.countNoop();
                return null;
            case DELETE:
                RequestWrapper<DeleteRequest> delete = wrap(new DeleteRequest(request.getIndex(), request.getType(), request.getId()));
                delete.setVersion(request.getVersion());
                delete.setVersionType(VersionType.INTERNAL);
                delete.setParent(request.getParent());
                delete.setRouting(request.getRouting());
                return delete;
            default:
                throw new IllegalArgumentException("Unsupported operation type change from [" + oldOpType + "] to [" + newOpType + "]");
            }
        }

        protected abstract void scriptChangedIndex(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedType(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedId(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedVersion(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedRouting(RequestWrapper<?> request, Object to);

        protected abstract void scriptChangedParent(RequestWrapper<?> request, Object to);

    }

    public enum OpType {

        NOOP("noop"),
        INDEX("index"),
        DELETE("delete");

        private final String id;

        OpType(String id) {
            this.id = id;
        }

        public static OpType fromString(String opType) {
            String lowerOpType = opType.toLowerCase(Locale.ROOT);
            switch (lowerOpType) {
                case "noop":
                    return OpType.NOOP;
                case "index":
                    return OpType.INDEX;
                case "delete":
                    return OpType.DELETE;
                default:
                    throw new IllegalArgumentException("Operation type [" + lowerOpType + "] not allowed, only " +
                            Arrays.toString(values()) + " are allowed");
            }
        }

        @Override
        public String toString() {
            return id.toLowerCase(Locale.ROOT);
        }
    }
}
