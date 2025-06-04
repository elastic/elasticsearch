/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlMetadata;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.exception.ExceptionsHelper.status;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;

public class ResultsPersisterService {
    /**
     * List of rest statuses that we consider irrecoverable
     */
    public static final Set<RestStatus> IRRECOVERABLE_REST_STATUSES = Set.of(
        RestStatus.GONE,
        RestStatus.NOT_IMPLEMENTED,
        // Not found is returned when we require an alias but the index is NOT an alias.
        RestStatus.NOT_FOUND,
        RestStatus.BAD_REQUEST,
        RestStatus.UNAUTHORIZED,
        RestStatus.FORBIDDEN,
        RestStatus.METHOD_NOT_ALLOWED,
        RestStatus.NOT_ACCEPTABLE
    );

    private static final Logger LOGGER = LogManager.getLogger(ResultsPersisterService.class);

    public static final Setting<Integer> PERSIST_RESULTS_MAX_RETRIES = Setting.intSetting(
        "xpack.ml.persist_results_max_retries",
        20,
        0,
        50,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );
    private static final int MAX_RETRY_SLEEP_MILLIS = (int) Duration.ofMinutes(15).toMillis();
    private static final int MIN_RETRY_SLEEP_MILLIS = 50;
    // Having an exponent higher than this causes integer overflow
    private static final int MAX_RETRY_EXPONENT = 24;

    private final ThreadPool threadPool;
    private final OriginSettingClient client;
    private final Map<Object, RetryableAction<?>> onGoingRetryableSearchActions = ConcurrentCollections.newConcurrentMap();
    private final Map<Object, RetryableAction<?>> onGoingRetryableBulkActions = ConcurrentCollections.newConcurrentMap();
    private volatile int maxFailureRetries;
    private volatile boolean isShutdown = false;
    private volatile boolean isResetMode = false;

    // Visible for testing
    public ResultsPersisterService(ThreadPool threadPool, OriginSettingClient client, ClusterService clusterService, Settings settings) {
        this.threadPool = threadPool;
        this.client = client;
        this.maxFailureRetries = PERSIST_RESULTS_MAX_RETRIES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PERSIST_RESULTS_MAX_RETRIES, this::setMaxFailureRetries);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                shutdown();
            }
        });
        clusterService.addListener((event) -> {
            if (event.metadataChanged()) {
                isResetMode = MlMetadata.getMlMetadata(event.state()).isResetMode();
                if (isResetMode) {
                    final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("Reset mode has been enabled");
                    for (RetryableAction<?> action : onGoingRetryableBulkActions.values()) {
                        action.cancel(exception);
                    }
                    onGoingRetryableBulkActions.clear();
                }
            }
        });
    }

    void shutdown() {
        isShutdown = true;
        if (onGoingRetryableSearchActions.isEmpty() && onGoingRetryableBulkActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("Node is shutting down");
        for (RetryableAction<?> action : onGoingRetryableSearchActions.values()) {
            action.cancel(exception);
        }
        for (RetryableAction<?> action : onGoingRetryableBulkActions.values()) {
            action.cancel(exception);
        }
        onGoingRetryableSearchActions.clear();
        onGoingRetryableBulkActions.clear();
    }

    void setMaxFailureRetries(int value) {
        this.maxFailureRetries = value;
    }

    public BulkResponse indexWithRetry(
        String jobId,
        String indexName,
        ToXContent object,
        ToXContent.Params params,
        WriteRequest.RefreshPolicy refreshPolicy,
        String id,
        boolean requireAlias,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler
    ) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        try (XContentBuilder content = object.toXContent(XContentFactory.jsonBuilder(), params)) {
            bulkRequest.add(new IndexRequest(indexName).id(id).source(content).setRequireAlias(requireAlias));
        }
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, retryMsgHandler);
    }

    public void indexWithRetry(
        String jobId,
        String indexName,
        ToXContent object,
        ToXContent.Params params,
        WriteRequest.RefreshPolicy refreshPolicy,
        String id,
        boolean requireAlias,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler,
        ActionListener<BulkResponse> finalListener
    ) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        try (XContentBuilder content = object.toXContent(XContentFactory.jsonBuilder(), params)) {
            bulkRequest.add(new IndexRequest(indexName).id(id).source(content).setRequireAlias(requireAlias));
        }
        bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, retryMsgHandler, finalListener);
    }

    public BulkResponse bulkIndexWithRetry(
        BulkRequest bulkRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler
    ) {
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, retryMsgHandler, client::bulk);
    }

    public void bulkIndexWithRetry(
        BulkRequest bulkRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler,
        ActionListener<BulkResponse> finalListener
    ) {
        if (isShutdown || isResetMode) {
            finalListener.onFailure(
                new ElasticsearchStatusException(
                    "Bulk indexing has failed as {}",
                    RestStatus.TOO_MANY_REQUESTS,
                    isShutdown ? "node is shutting down." : "machine learning feature is being reset."
                )
            );
            return;
        }
        bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, retryMsgHandler, client::bulk, finalListener);
    }

    public BulkResponse bulkIndexWithHeadersWithRetry(
        Map<String, String> headers,
        BulkRequest bulkRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler
    ) {
        return bulkIndexWithRetry(
            bulkRequest,
            jobId,
            shouldRetry,
            retryMsgHandler,
            (providedBulkRequest, listener) -> ClientHelper.executeWithHeadersAsync(
                headers,
                ClientHelper.ML_ORIGIN,
                client,
                TransportBulkAction.TYPE,
                providedBulkRequest,
                listener
            )
        );
    }

    private BulkResponse bulkIndexWithRetry(
        BulkRequest bulkRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> actionExecutor
    ) {
        if (isShutdown || isResetMode) {
            throw new ElasticsearchStatusException(
                "Bulk indexing has failed as {}",
                RestStatus.TOO_MANY_REQUESTS,
                isShutdown ? "node is shutting down." : "machine learning feature is being reset."
            );
        }
        final PlainActionFuture<BulkResponse> getResponseFuture = new PlainActionFuture<>();
        bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, retryMsgHandler, actionExecutor, getResponseFuture);
        return getResponseFuture.actionGet();
    }

    private void bulkIndexWithRetry(
        BulkRequest bulkRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> actionExecutor,
        ActionListener<BulkResponse> finalListener
    ) {
        final Object key = new Object();
        final ActionListener<BulkResponse> removeListener = ActionListener.runBefore(
            finalListener,
            () -> onGoingRetryableBulkActions.remove(key)
        );
        BulkRetryableAction bulkRetryableAction = new BulkRetryableAction(
            jobId,
            new BulkRequestRewriter(bulkRequest),
            () -> (isShutdown == false && isResetMode == false) && shouldRetry.get(),
            retryMsgHandler,
            actionExecutor,
            removeListener
        );
        onGoingRetryableBulkActions.put(key, bulkRetryableAction);
        bulkRetryableAction.run();
        if (isShutdown || isResetMode) {
            bulkRetryableAction.cancel(
                new CancellableThreads.ExecutionCancelledException(
                    isShutdown ? "Node is shutting down" : "Machine learning feature is being reset"
                )
            );
        }
    }

    public SearchResponse searchWithRetry(
        SearchRequest searchRequest,
        String jobId,
        Supplier<Boolean> shouldRetry,
        Consumer<String> retryMsgHandler
    ) {
        final PlainActionFuture<SearchResponse> getResponse = new PlainActionFuture<>();
        final Object key = new Object();
        final ActionListener<SearchResponse> removeListener = ActionListener.runBefore(
            getResponse,
            () -> onGoingRetryableSearchActions.remove(key)
        );
        SearchRetryableAction mlRetryableAction = new SearchRetryableAction(
            jobId,
            searchRequest,
            client,
            () -> (isShutdown == false) && shouldRetry.get(),
            retryMsgHandler,
            removeListener.delegateFailure((l, r) -> {
                r.mustIncRef();
                l.onResponse(r);
            })
        );
        onGoingRetryableSearchActions.put(key, mlRetryableAction);
        mlRetryableAction.run();
        if (isShutdown) {
            mlRetryableAction.cancel(new CancellableThreads.ExecutionCancelledException("Node is shutting down"));
        }
        return getResponse.actionGet();
    }

    static class RecoverableException extends Exception {}

    static class IrrecoverableException extends ElasticsearchStatusException {
        IrrecoverableException(String msg, RestStatus status, Throwable cause, Object... args) {
            super(msg, status, cause, args);
        }
    }

    /**
     * @param ex The exception to check
     * @return true when the failure will persist no matter how many times we retry.
     */
    private static boolean isIrrecoverable(Exception ex) {
        Throwable t = ExceptionsHelper.unwrapCause(ex);
        return IRRECOVERABLE_REST_STATUSES.contains(status(t));
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    static class BulkRequestRewriter {
        private volatile BulkRequest bulkRequest;

        BulkRequestRewriter(BulkRequest initialRequest) {
            this.bulkRequest = initialRequest;
        }

        void rewriteRequest(BulkResponse bulkResponse) {
            if (bulkResponse.hasFailures() == false) {
                return;
            }
            bulkRequest = buildNewRequestFromFailures(bulkRequest, bulkResponse);
        }

        BulkRequest getBulkRequest() {
            return bulkRequest;
        }

    }

    private class BulkRetryableAction extends MlRetryableAction<BulkRequest, BulkResponse> {
        private final BulkRequestRewriter bulkRequestRewriter;

        BulkRetryableAction(
            String jobId,
            BulkRequestRewriter bulkRequestRewriter,
            Supplier<Boolean> shouldRetry,
            Consumer<String> msgHandler,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> actionExecutor,
            ActionListener<BulkResponse> listener
        ) {
            super(
                jobId,
                shouldRetry,
                msgHandler,
                (request, retryableListener) -> actionExecutor.accept(request, ActionListener.wrap(bulkResponse -> {
                    if (bulkResponse.hasFailures() == false) {
                        retryableListener.onResponse(bulkResponse);
                        return;
                    }
                    for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                        if (itemResponse.isFailed() && isIrrecoverable(itemResponse.getFailure().getCause())) {
                            Throwable unwrappedParticular = ExceptionsHelper.unwrapCause(itemResponse.getFailure().getCause());
                            LOGGER.warn(
                                () -> format(
                                    "[%s] experienced failure that cannot be automatically retried. Bulk failure message [%s]",
                                    jobId,
                                    bulkResponse.buildFailureMessage()
                                ),
                                unwrappedParticular
                            );
                            retryableListener.onFailure(
                                new IrrecoverableException(
                                    "{} experienced failure that cannot be automatically retried. See logs for bulk failures",
                                    status(unwrappedParticular),
                                    unwrappedParticular,
                                    jobId
                                )
                            );
                            return;
                        }
                    }
                    bulkRequestRewriter.rewriteRequest(bulkResponse);
                    // Let the listener attempt again with the new bulk request
                    retryableListener.onFailure(new RecoverableException());
                }, retryableListener::onFailure)),
                listener
            );
            this.bulkRequestRewriter = bulkRequestRewriter;
        }

        @Override
        public BulkRequest buildRequest() {
            return bulkRequestRewriter.getBulkRequest();
        }

        @Override
        public String getName() {
            return "index";
        }

    }

    private class SearchRetryableAction extends MlRetryableAction<SearchRequest, SearchResponse> {

        private final SearchRequest searchRequest;

        SearchRetryableAction(
            String jobId,
            SearchRequest searchRequest,
            // Pass the client to work around https://bugs.eclipse.org/bugs/show_bug.cgi?id=569557
            OriginSettingClient client,
            Supplier<Boolean> shouldRetry,
            Consumer<String> msgHandler,
            ActionListener<SearchResponse> listener
        ) {
            super(
                jobId,
                shouldRetry,
                msgHandler,
                (request, retryableListener) -> client.search(request, ActionListener.wrap(searchResponse -> {
                    if (RestStatus.OK.equals(searchResponse.status())) {
                        retryableListener.onResponse(searchResponse);
                        return;
                    }
                    retryableListener.onFailure(
                        new ElasticsearchStatusException("search failed with status {}", searchResponse.status(), searchResponse.status())
                    );
                }, retryableListener::onFailure)),
                listener
            );
            this.searchRequest = searchRequest;
        }

        @Override
        public SearchRequest buildRequest() {
            return searchRequest;
        }

        @Override
        public String getName() {
            return "search";
        }
    }

    // This encapsulates a retryable action that implements our custom backoff retry logic
    private abstract class MlRetryableAction<Request, Response> extends RetryableAction<Response> {

        final String jobId;
        final Supplier<Boolean> shouldRetry;
        final Consumer<String> msgHandler;
        final BiConsumer<Request, ActionListener<Response>> action;
        volatile int currentAttempt = 0;
        volatile long currentMax = MIN_RETRY_SLEEP_MILLIS;

        MlRetryableAction(
            String jobId,
            Supplier<Boolean> shouldRetry,
            Consumer<String> msgHandler,
            BiConsumer<Request, ActionListener<Response>> action,
            ActionListener<Response> listener
        ) {
            super(
                LOGGER,
                threadPool,
                TimeValue.timeValueMillis(MIN_RETRY_SLEEP_MILLIS),
                TimeValue.MAX_VALUE,
                listener,
                threadPool.executor(UTILITY_THREAD_POOL_NAME)
            );
            this.jobId = jobId;
            this.shouldRetry = shouldRetry;
            this.msgHandler = msgHandler;
            this.action = action;
        }

        public abstract Request buildRequest();

        public abstract String getName();

        @Override
        public void tryAction(ActionListener<Response> listener) {
            currentAttempt++;
            action.accept(buildRequest(), listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (isIrrecoverable(e)) {
                LOGGER.warn(() -> "[" + jobId + "] experienced failure that cannot be automatically retried", e);
                return false;
            }

            // If the outside conditions have changed and retries are no longer needed, do not retry.
            if (shouldRetry.get() == false) {
                LOGGER.info(() -> format("[%s] should not retry %s after [%s] attempts", jobId, getName(), currentAttempt), e);
                return false;
            }

            // If the configured maximum number of retries has been reached, do not retry.
            if (currentAttempt > maxFailureRetries) {
                LOGGER.warn(() -> format("[%s] failed to %s after [%s] attempts.", jobId, getName(), currentAttempt), e);
                return false;
            }
            return true;
        }

        @Override
        protected long calculateDelayBound(long previousDelayBound) {
            // Exponential backoff calculation taken from: https://en.wikipedia.org/wiki/Exponential_backoff
            int uncappedBackoff = ((1 << Math.min(currentAttempt, MAX_RETRY_EXPONENT)) - 1) * (50);
            currentMax = Math.min(uncappedBackoff, MAX_RETRY_SLEEP_MILLIS);
            String msg = format("failed to %s after [%s] attempts. Will attempt again.", getName(), currentAttempt);
            LOGGER.warn(() -> format("[%s] %s", jobId, msg));
            msgHandler.accept(msg);
            // RetryableAction randomizes in the interval [currentMax/2 ; currentMax].
            // Its good to have a random window along the exponentially increasing curve
            // so that not all bulk requests rest for the same amount of time
            return currentMax;
        }

        @Override
        public void cancel(Exception e) {
            super.cancel(e);
            LOGGER.debug(() -> format("[%s] retrying cancelled for action [%s]", jobId, getName()), e);
        }
    }

    static BulkRequest buildNewRequestFromFailures(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        // If we failed, lets set the bulkRequest to be a collection of the failed requests
        BulkRequest bulkRequestOfFailures = new BulkRequest();
        Set<String> failedDocIds = Arrays.stream(bulkResponse.getItems())
            .filter(BulkItemResponse::isFailed)
            .map(BulkItemResponse::getId)
            .collect(Collectors.toSet());
        bulkRequest.requests().forEach(docWriteRequest -> {
            if (failedDocIds.contains(docWriteRequest.id())) {
                if (docWriteRequest instanceof IndexRequest ir) {
                    ir.reset();
                }
                bulkRequestOfFailures.add(docWriteRequest);
            }
        });
        return bulkRequestOfFailures;
    }

}
