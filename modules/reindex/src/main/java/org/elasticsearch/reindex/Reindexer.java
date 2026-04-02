/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeBulkByScrollRequest;
import org.elasticsearch.index.reindex.ResumeBulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.reindex.remote.RemoteReindexingUtils;
import org.elasticsearch.reindex.remote.RemoteScrollablePaginatedHitSource;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.ReindexMetadata;
import org.elasticsearch.script.ReindexScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static org.elasticsearch.common.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.index.VersionType.INTERNAL;
import static org.elasticsearch.reindex.ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.closePit;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.openPit;

public class Reindexer {

    private static final Logger logger = LogManager.getLogger(Reindexer.class);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final Client client;
    private final ThreadPool threadPool;
    private final ScriptService scriptService;
    private final ReindexSslConfig reindexSslConfig;
    private final ReindexMetrics reindexMetrics;
    private final TaskManager taskManager;
    private final TransportService transportService;
    private final ReindexRelocationNodePicker relocationNodePicker;
    private final FeatureService featureService;

    Reindexer(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        ThreadPool threadPool,
        ScriptService scriptService,
        ReindexSslConfig reindexSslConfig,
        @Nullable ReindexMetrics reindexMetrics,
        TransportService transportService,
        ReindexRelocationNodePicker relocationNodePicker,
        FeatureService featureService
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.client = client;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.reindexSslConfig = reindexSslConfig;
        this.reindexMetrics = reindexMetrics;
        this.taskManager = transportService.getTaskManager(); // implicit null check
        this.transportService = transportService;
        this.relocationNodePicker = Objects.requireNonNull(relocationNodePicker);
        this.featureService = featureService;
    }

    public void initTask(BulkByScrollTask task, ReindexRequest request, ActionListener<Void> listener) {
        final ActionListener<Void> initListener = listener.delegateFailure((l, v) -> {
            initTaskForRelocationIfEnabled(task);
            l.onResponse(v);
        });
        BulkByPaginatedSearchParallelizationHelper.initTaskState(task, request, client, initListener);
    }

    public void execute(BulkByScrollTask task, ReindexRequest request, Client bulkClient, ActionListener<BulkByScrollResponse> listener) {
        // todo: move relocations to BulkByPaginatedSearchParallelizationHelper rather than having it in Reindexer, makes it generic
        // for update-by-query and delete-by-query
        final ActionListener<BulkByScrollResponse> responseListener = wrapWithMetrics(
            listenerWithRelocations(task, request, listener),
            reindexMetrics,
            task,
            request
        );

        final boolean isRemote = request.getRemoteInfo() != null;
        Consumer<Version> workerAction = createWorkerAction(task, request, bulkClient, responseListener);

        // Point-in-time searching is disabled, so default to scroll
        if (featureService.clusterHasFeature(clusterService.state(), REINDEX_PIT_SEARCH_FEATURE) == false) {
            executePaginatedSearch(task, request, responseListener, workerAction, null);
        }
        /**
         * Point-in-time searching is enabled
         * As this is a request to reindex from remote, we need to determine the remote version prior to execution
         * NB {@link ReindexRequest} forbids remote requests and slices > 1, so we're guaranteed to be running on the only slice
         */
        else if (isRemote) {
            lookupRemoteVersionAndExecute(task, request, bulkClient, responseListener, workerAction);
        }
        // Point-in-time searching is enabled, and this is a local request
        else {
            openPitAndExecute(task, request, bulkClient, responseListener);
        }
    }

    /**
     * Creates the worker action that runs the reindex.
     * When PIT is used, the listener should include runAfter logic to close the PIT.
     */
    private Consumer<Version> createWorkerAction(
        BulkByScrollTask task,
        ReindexRequest request,
        Client bulkClient,
        ActionListener<BulkByScrollResponse> listener
    ) {
        return remoteVersion -> {
            ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(client, clusterService.localNode(), task);
            ParentTaskAssigningClient assigningBulkClient = new ParentTaskAssigningClient(bulkClient, clusterService.localNode(), task);
            AsyncIndexBySearchAction searchAction = new AsyncIndexBySearchAction(
                task,
                logger,
                assigningClient,
                assigningBulkClient,
                threadPool,
                scriptService,
                projectResolver.getProjectState(clusterService.state()),
                reindexSslConfig,
                request,
                listener,
                remoteVersion
            );
            searchAction.start();
        };
    }

    /**
     * Runs the sliced action
     */
    private void executePaginatedSearch(
        BulkByScrollTask task,
        ReindexRequest request,
        ActionListener<BulkByScrollResponse> listener,
        Consumer<Version> workerAction,
        @Nullable Version remoteVersion
    ) {
        BulkByPaginatedSearchParallelizationHelper.executeSlicedAction(
            task,
            request,
            ReindexAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            remoteVersion,
            workerAction
        );
    }

    /**
     * Returns the keep-alive duration for PIT. Uses the request's scroll time when set, otherwise defaults to 5 minutes.
     * TODO - https://github.com/elastic/elasticsearch-team/issues/2334
     */
    private static TimeValue pitKeepAlive(ReindexRequest request) {
        return request.getScrollTime() != null ? request.getScrollTime() : TimeValue.timeValueMinutes(5);
    }

    /**
     * Opens a PIT on the local cluster, runs the sliced action, and closes the PIT when done.
     */
    private void openPitAndExecute(
        BulkByScrollTask task,
        ReindexRequest request,
        Client bulkClient,
        ActionListener<BulkByScrollResponse> listener
    ) {
        SearchRequest searchRequest = request.getSearchRequest();
        String[] indices = searchRequest.indices();

        // The routing and preference parameters can be set for a PIT request. However, scroll currently does not use these,
        // so for parity we assert here in case that changes
        assert searchRequest.routing() == null : "Routing is set in the search request, but is not being used when opening the PIT.";
        assert searchRequest.preference() == null : "Preference is set in the search request, but is not being used when opening the PIT.";
        assert searchRequest.allowPartialSearchResults() == null || searchRequest.allowPartialSearchResults() == false
            : "allow_partial_search_results must be false when opening a PIT to match scroll search behavior";

        // TODO - Do we need to set the IndexFilter field here? https://github.com/elastic/elasticsearch-team/issues/2392
        OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest(indices).indicesOptions(searchRequest.indicesOptions())
            .keepAlive(pitKeepAlive(request))
            .allowPartialSearchResults(false);

        // NB this is a local request, so we call the TransportAction rather than issuing a REST call
        client.execute(TransportOpenPointInTimeAction.TYPE, pitRequest, listener.delegateFailureAndWrap((l, pitResponse) -> {
            BytesReference pitId = pitResponse.getPointInTimeId();
            ActionListener<BulkByScrollResponse> listenerWithClosePit = ActionListener.runAfter(
                l,
                () -> client.execute(
                    TransportClosePointInTimeAction.TYPE,
                    new ClosePointInTimeRequest(pitId),
                    ActionListener.wrap(r -> {}, e -> logger.warn("Failed to close local PIT", e))
                )
            );
            Consumer<Version> workerActionWithClosePit = createWorkerAction(task, request, bulkClient, listenerWithClosePit);
            // TODO - Pass the point-in-time ID into the BulkByPaginatedSearchParallelizationHelper to be used
            executePaginatedSearch(task, request, listenerWithClosePit, workerActionWithClosePit, null);
        }));
    }

    /**
     * Looks up the remote cluster version when reindexing from a remote source.
     * If the remote supports PIT (7.10.0+), this opens a PIT, runs the sliced action, and closes the PIT when done.
     * Otherwise, the default search method is scroll.
     * The RestClient used for lookup (and PIT open/close when applicable) is closed after completion.
     */
    private void lookupRemoteVersionAndExecute(
        BulkByScrollTask task,
        ReindexRequest request,
        Client bulkClient,
        ActionListener<BulkByScrollResponse> listener,
        Consumer<Version> workerAction
    ) {
        RemoteInfo remoteInfo = request.getRemoteInfo();
        assert reindexSslConfig != null : "Reindex ssl config must be set";
        RestClient restClient = buildRestClient(remoteInfo, reindexSslConfig, task.getId(), synchronizedList(new ArrayList<>()));
        RejectAwareActionListener<Version> rejectAwareListener = new RejectAwareActionListener<>() {
            @Override
            public void onResponse(Version version) {
                boolean canUsePit = version.onOrAfter(Version.V_7_10_0);
                if (canUsePit) {
                    openRemotePitAndExecute(task, request, bulkClient, listener, restClient, version);
                }
                // Default to scroll-based search
                else {
                    closeRestClientAndRun(restClient, () -> executePaginatedSearch(task, request, listener, workerAction, version));
                }
            }

            @Override
            public void onFailure(Exception e) {
                closeRestClientAndRun(restClient, () -> listener.onFailure(e));
            }

            @Override
            public void onRejection(Exception e) {
                closeRestClientAndRun(restClient, () -> listener.onFailure(e));
            }
        };

        RemoteReindexingUtils.lookupRemoteVersionWithRetries(
            logger,
            exponentialBackoff(request.getRetryBackoffInitialTime(), request.getMaxRetries()),
            threadPool,
            restClient,
            // TODO - Do we want to pass in a countRetry runnable here to count the number of times we retry?
            // https://github.com/elastic/elasticsearch-team/issues/2382
            rejectAwareListener
        );
    }

    /**
     * Opens a PIT on the remote cluster, runs the sliced action, and closes the PIT when done.
     * The RestClient is closed after the PIT is closed.
     */
    private void openRemotePitAndExecute(
        BulkByScrollTask task,
        ReindexRequest request,
        Client bulkClient,
        ActionListener<BulkByScrollResponse> listenerWithRelocations,
        RestClient restClient,
        Version remoteVersion
    ) {
        SearchRequest searchRequest = request.getSearchRequest();
        String[] indices = searchRequest.indices();
        // Sends a REST request to the remote node to open a PIT
        openPit(searchRequest, indices, pitKeepAlive(request), RejectAwareActionListener.wrap(pitId -> {
            ActionListener<BulkByScrollResponse> listenerWithClosePit = ActionListener.runAfter(
                listenerWithRelocations,
                () -> closePit(pitId, RejectAwareActionListener.wrap(v -> closeRestClientAndRun(restClient, () -> {}), e -> {
                    logger.warn("Failed to close remote PIT", e);
                    closeRestClientAndRun(restClient, () -> {});
                }, e -> {
                    logger.warn("Failed to close remote PIT (rejected)", e);
                    closeRestClientAndRun(restClient, () -> {});
                }), threadPool, restClient)
            );
            Consumer<Version> workerActionWithClosePit = createWorkerAction(task, request, bulkClient, listenerWithClosePit);
            // TODO - Pass the point-in-time ID into the BulkByPaginatedSearchParallelizationHelper to be used
            executePaginatedSearch(task, request, listenerWithClosePit, workerActionWithClosePit, remoteVersion);
        },
            e -> closeRestClientAndRun(restClient, () -> listenerWithRelocations.onFailure(e)),
            e -> closeRestClientAndRun(restClient, () -> listenerWithRelocations.onFailure(e))
        ), threadPool, restClient);
    }

    /**
     * Closes the RestClient on the generic thread pool (to avoid closing from the client's own thread), then runs the given action.
     */
    private void closeRestClientAndRun(RestClient restClient, Runnable onCompletion) {
        threadPool.generic().submit(() -> {
            try {
                restClient.close();
            } catch (IOException e) {
                logger.warn("Failed to close RestClient after version lookup", e);
            } finally {
                onCompletion.run();
            }
        });
    }

    /**
     * Wrap listener with reindex metrics. For sliced reindex, this should record once only when all slices complete
     * Visible for testing
     */
    static ActionListener<BulkByScrollResponse> wrapWithMetrics(
        ActionListener<BulkByScrollResponse> listener,
        @Nullable ReindexMetrics metrics,
        BulkByScrollTask task,
        ReindexRequest request
    ) {
        return wrapWithMetrics(listener, metrics, task, request, System::currentTimeMillis);
    }

    /** Overload accepting a clock supplier for testability. Visible for testing*/
    static ActionListener<BulkByScrollResponse> wrapWithMetrics(
        final ActionListener<BulkByScrollResponse> listener,
        final @Nullable ReindexMetrics metrics,
        final BulkByScrollTask task,
        final ReindexRequest request,
        final LongSupplier currentTimeMillisSupplier
    ) {
        if (metrics == null) {
            return listener;
        }
        if (task.isWorker() && task.getParentTaskId().isSet()) {
            // Do not record metrics for slice worker, only leader will record them when all slices are completed
            // Potentially add slice-level metrics for slice worker
            return listener;
        }
        final boolean isRemote = request.getRemoteInfo() != null;
        final ReindexMetrics.SlicingMode slicingMode = ReindexMetrics.resolveSlicingMode(request);
        return new ActionListener<>() {
            private void recordDuration() {
                // handles relocations
                long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(
                    currentTimeMillisSupplier.getAsLong() - task.relocationOrigin().originalStartTimeMillis()
                );
                metrics.recordTookTime(elapsedTimeSeconds, isRemote, slicingMode);
            }

            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                if (bulkByScrollResponse.getTaskResumeInfo().isPresent()) {
                    // Task will be relocated to a different node
                    // Do not record metrics on the source node, the destination node will record metrics when the relocated task completes
                    assert bulkByScrollResponse.getBulkFailures().isEmpty() : "bulk failures should be empty if relocating";
                    assert bulkByScrollResponse.getSearchFailures().isEmpty() : "search failures should be empty if relocating";
                    listener.onResponse(bulkByScrollResponse);
                    return;
                }
                try {
                    var searchExceptionSample = Optional.ofNullable(bulkByScrollResponse.getSearchFailures())
                        .stream()
                        .flatMap(List::stream)
                        .map(PaginatedSearchFailure::getReason)
                        .findFirst();
                    var bulkExceptionSample = Optional.ofNullable(bulkByScrollResponse.getBulkFailures())
                        .stream()
                        .flatMap(List::stream)
                        .map(BulkItemResponse.Failure::getCause)
                        .findFirst();
                    if (searchExceptionSample.isPresent() || bulkExceptionSample.isPresent()) {
                        Throwable e = searchExceptionSample.orElseGet(bulkExceptionSample::get);
                        metrics.recordFailure(isRemote, slicingMode, e);
                    } else {
                        metrics.recordSuccess(isRemote, slicingMode);
                    }
                    listener.onResponse(bulkByScrollResponse);
                } finally {
                    recordDuration();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    metrics.recordFailure(isRemote, slicingMode, e);
                    listener.onFailure(e);
                } finally {
                    recordDuration();
                }
            }
        };
    }

    /** Wraps the listener with relocation handling (if applicable). Visible for testing. */
    ActionListener<BulkByScrollResponse> listenerWithRelocations(
        final BulkByScrollTask task,
        final ReindexRequest request,
        final ActionListener<BulkByScrollResponse> listener
    ) {
        final boolean isRelocationHandledByLeader = getReindexParent(task).isPresent();
        if (isRelocationHandledByLeader) {
            return listener;
        }
        return listener.delegateFailureAndWrap((l, response) -> {
            // note: isRelocationRequested will never be true if the entire cluster doesn't support relocations
            // note: getTaskResumeInfo will only be populated if there's a suitable node to relocate to
            if (task.isRelocationRequested() == false || response.getTaskResumeInfo().isEmpty()) {
                l.onResponse(response);
                return;
            }
            final String nodeToRelocateTo = task.isLeader()
                ? task.getLeaderState().getNodeToRelocateTo().orElse(null)
                : task.getWorkerState().getNodeToRelocateTo().orElse(null);
            assert nodeToRelocateTo != null : "node to relocate to should be set if taskResumeInfo is present";
            final DiscoveryNode nodeToRelocateToNode = clusterService.state().nodes().get(nodeToRelocateTo);
            if (nodeToRelocateToNode == null) {
                l.onFailure(
                    new IllegalStateException(Strings.format("Node %s to relocate to left cluster before relocation", nodeToRelocateTo))
                );
                return;
            }
            request.setResumeInfo(response.getTaskResumeInfo().get());
            final ResumeBulkByScrollRequest resumeRequest = new ResumeBulkByScrollRequest(request);
            final ActionListener<ResumeBulkByScrollResponse> relocationListener = ActionListener.wrap(resp -> {
                final var relocatedException = new TaskRelocatedException();
                relocatedException.setOriginalAndRelocatedTaskIdMetadata(
                    new TaskId(clusterService.localNode().getId(), task.getId()),
                    resp.getTaskId()
                );
                l.onFailure(relocatedException);
            }, l::onFailure);
            transportService.sendRequest(
                nodeToRelocateToNode,
                ResumeReindexAction.NAME,
                resumeRequest,
                new ActionListenerResponseHandler<>(relocationListener, ResumeBulkByScrollResponse::new, threadPool.generic())
            );
        });
    }

    private void initTaskForRelocationIfEnabled(final BulkByScrollTask task) {
        // todo: move initialization to BulkByPaginatedSearchParallelizationHelper rather than having it in Reindexer, makes it generic
        // for update-by-query and delete-by-query
        if (ReindexPlugin.REINDEX_RESILIENCE_ENABLED == false) {
            return;
        }
        // set up reindex relocation, specifically the supplier which says which node to relocate to.
        // we have 3 states to handle:
        // 1. leader which has >= 2 subslices: initialized with a centralized node picker. workers will fetch this and use it.
        // 2. worker which is one of many subslices: fetch the leader node picker and use it for relocation decisions.
        // 3. worker which is the only slice, no leader: set up its own node picker, call that, and when it's done, relocate itself.
        // n.b.
        // - workers with a leader only need the supplier to see if there is a node to relocate to, therefore, whether they should stop.
        // - leader then relocates the entire reindex once workers have completed.
        // - approach relies on the entire reindex (including subtasks) existing on the same node.
        if (task.isLeader()) {
            task.getLeaderState().setNodeToRelocateToSupplier(new NodeToRelocateToSupplier(clusterService, relocationNodePicker));
        } else {
            task.getWorkerState().setNodeToRelocateToSupplier(getWorkerNodeToRelocateToSupplier(task));
        }
    }

    /**
     * Thread-safe supplier of a possible node to relocate to, for the slice(s) of a reindex task.
     * Once a node is selected for relocation, it will be returned to all invocations.
     */
    private static class NodeToRelocateToSupplier implements Supplier<Optional<String>> {

        private final ClusterService clusterService;
        private final ReindexRelocationNodePicker relocationNodePicker;
        private final AtomicReference<String> nodeToRelocateTo;

        NodeToRelocateToSupplier(final ClusterService clusterService, final ReindexRelocationNodePicker relocationNodePicker) {
            this.clusterService = Objects.requireNonNull(clusterService);
            this.relocationNodePicker = Objects.requireNonNull(relocationNodePicker);
            this.nodeToRelocateTo = new AtomicReference<>();
        }

        @Override
        public Optional<String> get() {
            final String currentNodeToRelocateTo = this.nodeToRelocateTo.get();
            if (currentNodeToRelocateTo != null) { // node already selected
                return Optional.of(currentNodeToRelocateTo);
            }
            final ClusterState clusterState = clusterService.state(); // capture so isn't updated while we read both nodes and metadata
            final String newNodeToRelocateTo = relocationNodePicker.pickNode(clusterState.nodes(), clusterState.metadata().nodeShutdowns())
                .orElse(null);
            if (newNodeToRelocateTo == null) { // we didn't find a node to relocate to, but maybe another thread did by racing.
                return Optional.ofNullable(this.nodeToRelocateTo.get());
            }
            return Optional.of(
                this.nodeToRelocateTo.updateAndGet(
                    current -> current == null
                        ? newNodeToRelocateTo // this thread set the value so return that
                        : current // another thread set the value, so return that
                )
            );
        }
    }

    /**
     * Returns the parent {@link BulkByScrollTask} leader if this task is a worker whose parent will handle relocation on its behalf.
     * A parent task ID can also refer to an external caller (e.g. enrich, system migration) -- those don't handle relocation.
     * N.b. relies on reindex subtasks existing on the same node as the parent.
     */
    private Optional<BulkByScrollTask> getReindexParent(final BulkByScrollTask task) {
        if (task.isWorker() == false || task.getParentTaskId().isSet() == false) {
            return Optional.empty();
        }
        final CancellableTask parent = taskManager.getCancellableTasks().get(task.getParentTaskId().getId());
        if (parent instanceof BulkByScrollTask parentBbs && parentBbs.isLeader()) {
            return Optional.of(parentBbs);
        }
        return Optional.empty();
    }

    private Supplier<Optional<String>> getWorkerNodeToRelocateToSupplier(final BulkByScrollTask workerTask) {
        assert workerTask.isWorker() : "task should be a worker";
        return getReindexParent(workerTask).map(leader -> (Supplier<Optional<String>>) leader.getLeaderState()::getNodeToRelocateTo)
            // we don't need a thread-safe nodeToRelocateToSupplier for non-sliced, but re-using leads to less code
            .orElseGet(() -> new NodeToRelocateToSupplier(clusterService, relocationNodePicker));
    }

    /**
     * Build the {@link RestClient} used for reindexing from remote clusters.
     * @param remoteInfo connection information for the remote cluster
     * @param sslConfig configuration for potential outgoing HTTPS connections
     * @param taskId the id of the current task. This is added to the thread name for easier tracking
     * @param threadCollector a list in which we collect all the threads created by the client
     */
    static RestClient buildRestClient(RemoteInfo remoteInfo, ReindexSslConfig sslConfig, long taskId, List<Thread> threadCollector) {
        Header[] clientHeaders = new Header[remoteInfo.getHeaders().size()];
        int i = 0;
        for (Map.Entry<String, String> header : remoteInfo.getHeaders().entrySet()) {
            clientHeaders[i++] = new BasicHeader(header.getKey(), header.getValue());
        }
        final RestClientBuilder builder = RestClient.builder(
            new HttpHost(remoteInfo.getHost(), remoteInfo.getPort(), remoteInfo.getScheme())
        ).setDefaultHeaders(clientHeaders).setRequestConfigCallback(c -> {
            c.setConnectTimeout(Math.toIntExact(remoteInfo.getConnectTimeout().millis()));
            c.setSocketTimeout(Math.toIntExact(remoteInfo.getSocketTimeout().millis()));
            return c;
        }).setHttpClientConfigCallback(c -> {
            // Enable basic auth if it is configured
            if (remoteInfo.getUsername() != null) {
                UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
                    remoteInfo.getUsername(),
                    remoteInfo.getPassword().toString()
                );
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, creds);
                c.setDefaultCredentialsProvider(credentialsProvider);
            }
            // Stick the task id in the thread name so we can track down tasks from stack traces
            AtomicInteger threads = new AtomicInteger();
            c.setThreadFactory(r -> {
                String name = "es-client-" + taskId + "-" + threads.getAndIncrement();
                Thread t = new Thread(r, name);
                threadCollector.add(t);
                return t;
            });
            // Limit ourselves to one reactor thread because for now the search process is single threaded.
            c.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
            c.setSSLStrategy(sslConfig.getStrategy());
            return c;
        });
        if (Strings.hasLength(remoteInfo.getPathPrefix()) && "/".equals(remoteInfo.getPathPrefix()) == false) {
            builder.setPathPrefix(remoteInfo.getPathPrefix());
        }
        return builder.build();
    }

    /**
     * Simple implementation of reindex using scrolling and bulk. There are tons
     * of optimizations that can be done on certain types of reindex requests
     * but this makes no attempt to do any of them so it can be as simple
     * possible.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<ReindexRequest, TransportReindexAction> {
        /**
         * Mapper for the {@code _id} of the destination index used to
         * normalize {@code _id}s landing in the index.
         */
        private final IdFieldMapper destinationIndexIdMapper;

        /**
         * List of threads created by this process. Usually actions don't create threads in Elasticsearch. Instead they use the builtin
         * {@link ThreadPool}s. But reindex-from-remote uses Elasticsearch's {@link RestClient} which doesn't use the
         * {@linkplain ThreadPool}s because it uses httpasyncclient. It'd be a ton of trouble to work around creating those threads. So
         * instead we let it create threads but we watch them carefully and assert that they are dead when the process is over.
         */
        private List<Thread> createdThreads = emptyList();

        /**
         * Version of the remote cluster when reindexing from remote, or null when reindexing locally.
         */
        private final Version remoteVersion;

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient searchClient,
            ParentTaskAssigningClient bulkClient,
            ThreadPool threadPool,
            ScriptService scriptService,
            ProjectState state,
            ReindexSslConfig sslConfig,
            ReindexRequest request,
            ActionListener<BulkByScrollResponse> listener,
            @Nullable Version remoteVersion
        ) {
            super(
                task,
                /*
                 * We only need the source version if we're going to use it when write and we only do that when the destination request uses
                 * external versioning.
                 */
                request.getDestination().versionType() != VersionType.INTERNAL,
                false,
                true,
                logger,
                searchClient,
                bulkClient,
                threadPool,
                request,
                listener,
                scriptService,
                sslConfig
            );
            this.destinationIndexIdMapper = destinationIndexMode(state).idFieldMapperWithoutFieldData();
            this.remoteVersion = remoteVersion;
        }

        private IndexMode destinationIndexMode(ProjectState state) {
            ProjectMetadata projectMetadata = state.metadata();
            IndexMetadata destMeta = projectMetadata.index(mainRequest.getDestination().index());
            if (destMeta != null) {
                return IndexSettings.MODE.get(destMeta.getSettings());
            }
            String template = MetadataIndexTemplateService.findV2Template(projectMetadata, mainRequest.getDestination().index(), false);
            if (template == null) {
                return IndexMode.STANDARD;
            }
            Settings settings = MetadataIndexTemplateService.resolveSettings(projectMetadata, template);
            // We retrieve the setting without performing any validation because that the template has already been validated
            return IndexMode.fromIndexSettingsWithoutValidation(settings);
        }

        @Override
        protected PaginatedHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy, SearchRequest searchRequest) {
            if (mainRequest.getRemoteInfo() != null) {
                RemoteInfo remoteInfo = mainRequest.getRemoteInfo();
                createdThreads = synchronizedList(new ArrayList<>());
                assert sslConfig != null : "Reindex ssl config must be set";
                RestClient restClient = buildRestClient(remoteInfo, sslConfig, task.getId(), createdThreads);
                return new RemoteScrollablePaginatedHitSource(
                    logger,
                    backoffPolicy,
                    threadPool,
                    worker::countSearchRetry,
                    this::onScrollResponse,
                    this::finishHim,
                    restClient,
                    remoteInfo,
                    searchRequest,
                    remoteVersion
                );
            }
            return super.buildScrollableResultSource(backoffPolicy, searchRequest);
        }

        @Override
        protected void finishHim(
            Exception failure,
            List<BulkItemResponse.Failure> indexingFailures,
            List<PaginatedSearchFailure> searchFailures,
            boolean timedOut
        ) {
            super.finishHim(failure, indexingFailures, searchFailures, timedOut);
            // A little extra paranoia so we log something if we leave any threads running
            for (Thread thread : createdThreads) {
                if (thread.isAlive()) {
                    assert false : "Failed to properly stop client thread [" + thread.getName() + "]";
                    logger.error("Failed to properly stop client thread [{}]", thread.getName());
                }
            }
        }

        @Override
        public BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                assert scriptService != null : "Script service must be set";
                return new ReindexScriptApplier(worker, scriptService, script, script.getParams(), threadPool::absoluteTimeInMillis);
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(PaginatedHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();

            // Copy the index from the request so we always write where it asked to write
            index.index(mainRequest.getDestination().index());

            /*
             * Internal versioning can just use what we copied from the destination request. Otherwise we assume we're using external
             * versioning and use the doc's version.
             */
            index.versionType(mainRequest.getDestination().versionType());
            if (index.versionType() == INTERNAL) {
                assert doc.getVersion() == -1 : "fetched version when we didn't have to";
                index.version(mainRequest.getDestination().version());
            } else {
                index.version(doc.getVersion());
            }

            // id and source always come from the found doc. Scripts can change them but they operate on the index request.
            index.id(destinationIndexIdMapper.reindexId(doc.getId()));

            // the source xcontent type and destination could be different
            final XContentType sourceXContentType = doc.getXContentType();
            final XContentType mainRequestXContentType = mainRequest.getDestination().getContentType();
            if (mainRequestXContentType != null && doc.getXContentType() != mainRequestXContentType) {
                // we need to convert
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY,
                        doc.getSource(),
                        sourceXContentType
                    );
                    XContentBuilder builder = XContentBuilder.builder(mainRequestXContentType.xContent())
                ) {
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                    index.source(BytesReference.bytes(builder), builder.contentType());
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "failed to convert hit from " + sourceXContentType + " to " + mainRequestXContentType,
                        e
                    );
                }
            } else {
                index.source(doc.getSource(), doc.getXContentType());
            }

            /*
             * The rest of the index request just has to be copied from the template. It may be changed later from scripts or the superclass
             * here on out operates on the index request rather than the template.
             */
            index.routing(mainRequest.getDestination().routing());
            index.setPipeline(mainRequest.getDestination().getPipeline());
            if (mainRequest.getDestination().opType() == DocWriteRequest.OpType.CREATE) {
                index.opType(mainRequest.getDestination().opType());
            }

            return wrap(index);
        }

        /**
         * Override the simple copy behavior to allow more fine grained control.
         */
        @Override
        protected void copyRouting(RequestWrapper<?> request, String routing) {
            String routingSpec = mainRequest.getDestination().routing();
            if (routingSpec == null) {
                super.copyRouting(request, routing);
                return;
            }
            if (routingSpec.startsWith("=")) {
                super.copyRouting(request, mainRequest.getDestination().routing().substring(1));
                return;
            }
            switch (routingSpec) {
                case "keep" -> super.copyRouting(request, routing);
                case "discard" -> super.copyRouting(request, null);
                default -> throw new IllegalArgumentException("Unsupported routing command");
            }
        }

        static class ReindexScriptApplier extends ScriptApplier<ReindexMetadata> {
            private ReindexScript.Factory reindex;

            ReindexScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params,
                LongSupplier nowInMillisSupplier
            ) {
                super(taskWorker, scriptService, script, params, nowInMillisSupplier);
            }

            @Override
            protected CtxMap<ReindexMetadata> execute(PaginatedHitSource.Hit doc, Map<String, Object> source) {
                if (reindex == null) {
                    reindex = scriptService.compile(script, ReindexScript.CONTEXT);
                }
                CtxMap<ReindexMetadata> ctxMap = new CtxMap<>(
                    source,
                    new ReindexMetadata(
                        doc.getIndex(),
                        doc.getId(),
                        doc.getVersion(),
                        doc.getRouting(),
                        INDEX,
                        nowInMillisSupplier.getAsLong()
                    )
                );
                reindex.newInstance(params, ctxMap).execute();
                return ctxMap;
            }

            @Override
            protected void updateRequest(RequestWrapper<?> request, ReindexMetadata metadata) {
                if (metadata.indexChanged()) {
                    request.setIndex(metadata.getIndex());
                }
                if (metadata.idChanged()) {
                    request.setId(metadata.getId());
                }
                if (metadata.versionChanged()) {
                    if (metadata.isVersionInternal()) {
                        request.setVersion(Versions.MATCH_ANY);
                        request.setVersionType(INTERNAL);
                    } else {
                        request.setVersion(metadata.getVersion());
                    }
                }
                /*
                 * Its important that routing comes after parent in case you want to
                 * change them both.
                 */
                if (metadata.routingChanged()) {
                    request.setRouting(metadata.getRouting());
                }
            }
        }
    }
}
