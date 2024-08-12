/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

public class TransportResolveClusterAction extends HandledTransportAction<ResolveClusterActionRequest, ResolveClusterActionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportResolveClusterAction.class);
    private static final String TRANSPORT_VERSION_ERROR_MESSAGE = "ResolveClusterAction requires at least Transport Version";

    public static final String NAME = "indices:admin/resolve/cluster";
    public static final ActionType<ResolveClusterActionResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<ResolveClusterActionResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        ResolveClusterActionResponse::new
    );

    private final Executor searchCoordinationExecutor;
    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final boolean ccsCheckCompatibility;

    @Inject
    public TransportResolveClusterAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(NAME, transportService, actionFilters, ResolveClusterActionRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.searchCoordinationExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        this.clusterService = clusterService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
    }

    @Override
    protected void doExecute(Task task, ResolveClusterActionRequest request, ActionListener<ResolveClusterActionResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        searchCoordinationExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
    }

    protected void doExecuteForked(Task task, ResolveClusterActionRequest request, ActionListener<ResolveClusterActionResponse> listener) {
        if (ccsCheckCompatibility) {
            checkCCSVersionCompatibility(request);
        }
        assert task instanceof CancellableTask;
        final CancellableTask resolveClusterTask = (CancellableTask) task;
        ClusterState clusterState = clusterService.state();
        Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(), request.indices());
        OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        Map<String, ResolveClusterInfo> clusterInfoMap = new ConcurrentHashMap<>();
        // add local cluster info if in scope of the index-expression from user
        if (localIndices != null) {
            try {
                boolean matchingIndices = hasMatchingIndices(localIndices, request.indicesOptions(), clusterState);
                clusterInfoMap.put(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    new ResolveClusterInfo(true, false, matchingIndices, Build.current())
                );
            } catch (IndexNotFoundException e) {
                clusterInfoMap.put(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, new ResolveClusterInfo(true, false, e.getMessage()));
            }
        } else if (request.isLocalIndicesRequested()) {
            // the localIndices entry can be null even when the user requested a local index, as the index resolution
            // process can remove them (see RemoteClusterActionRequest for more details), so if we get here, no matching
            // index was found and no security exception was thrown, so just set matchingIndices=false
            clusterInfoMap.put(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, new ResolveClusterInfo(true, false, false, Build.current()));
        }

        final var finishedOrCancelled = new AtomicBoolean();
        resolveClusterTask.addListener(() -> {
            if (finishedOrCancelled.compareAndSet(false, true)) {
                releaseResourcesOnCancel(clusterInfoMap);
            }
        });

        try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
            finishedOrCancelled.set(true);
            if (resolveClusterTask.notifyIfCancelled(listener)) {
                releaseResourcesOnCancel(clusterInfoMap);
            } else {
                listener.onResponse(new ResolveClusterActionResponse(clusterInfoMap));
            }
        })) {
            // make the cross-cluster calls
            for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                resolveClusterTask.ensureNotCancelled();
                String clusterAlias = remoteIndices.getKey();
                OriginalIndices originalIndices = remoteIndices.getValue();
                boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
                RemoteClusterClient remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                    clusterAlias,
                    searchCoordinationExecutor,
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                );
                var remoteRequest = new ResolveClusterActionRequest(originalIndices.indices(), request.indicesOptions());
                // allow cancellation requests to propagate to remote clusters
                remoteRequest.setParentTask(clusterService.localNode().getId(), task.getId());

                ActionListener<ResolveClusterActionResponse> remoteListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ResolveClusterActionResponse response) {
                        if (resolveClusterTask.isCancelled()) {
                            releaseResourcesOnCancel(clusterInfoMap);
                            return;
                        }
                        ResolveClusterInfo info = response.getResolveClusterInfo().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        if (info != null) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(info, skipUnavailable));
                        }
                        if (resolveClusterTask.isCancelled()) {
                            releaseResourcesOnCancel(clusterInfoMap);
                        }
                    }

                    @Override
                    public void onFailure(Exception failure) {
                        if (resolveClusterTask.isCancelled()) {
                            releaseResourcesOnCancel(clusterInfoMap);
                            return;
                        }
                        if (notConnectedError(failure)) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable));
                        } else if (ExceptionsHelper.unwrap(
                            failure,
                            ElasticsearchSecurityException.class
                        ) instanceof ElasticsearchSecurityException ese) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable, ese.getMessage()));
                        } else if (ExceptionsHelper.unwrap(failure, IndexNotFoundException.class) instanceof IndexNotFoundException infe) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable, infe.getMessage()));
                        } else {
                            Throwable cause = ExceptionsHelper.unwrapCause(failure);
                            // when querying an older cluster that does not have the _resolve/cluster endpoint, we will get
                            // this error at the Transport layer BEFORE it sends the request to the remote cluster, since there
                            // are version guards on the Writeables for this Action, namely ResolveClusterActionRequest.writeTo
                            if (cause instanceof UnsupportedOperationException
                                && cause.getMessage().contains(TRANSPORT_VERSION_ERROR_MESSAGE)) {
                                // Since this cluster does not have _resolve/cluster, we call the _resolve/index
                                // endpoint to fill in the matching_indices field of the response for that cluster
                                ResolveIndexAction.Request resolveIndexRequest = new ResolveIndexAction.Request(
                                    originalIndices.indices(),
                                    originalIndices.indicesOptions()
                                );
                                ActionListener<ResolveIndexAction.Response> resolveIndexActionListener = new ActionListener<>() {
                                    @Override
                                    public void onResponse(ResolveIndexAction.Response response) {
                                        boolean matchingIndices = response.getIndices().size() > 0
                                            || response.getAliases().size() > 0
                                            || response.getDataStreams().size() > 0;
                                        clusterInfoMap.put(
                                            clusterAlias,
                                            new ResolveClusterInfo(true, skipUnavailable, matchingIndices, null)
                                        );
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                                        clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable, cause.toString()));
                                        logger.warn(
                                            () -> Strings.format("Failure from _resolve/cluster lookup against cluster %s: ", clusterAlias),
                                            e
                                        );
                                    }
                                };
                                remoteClusterClient.execute(
                                    ResolveIndexAction.REMOTE_TYPE,
                                    resolveIndexRequest,
                                    ActionListener.releaseAfter(resolveIndexActionListener, refs.acquire())
                                );
                            } else {
                                // it is not clear that this error indicates that the cluster is disconnected, but it is hard to
                                // determine based on the error, so we default to false in the face of any error and report it
                                // back to the user for consideration
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable, cause.toString()));
                                logger.warn(
                                    () -> Strings.format("Failure from _resolve/cluster lookup against cluster %s: ", clusterAlias),
                                    failure
                                );
                            }
                        }
                        if (resolveClusterTask.isCancelled()) {
                            releaseResourcesOnCancel(clusterInfoMap);
                        }
                    }
                };
                remoteClusterClient.execute(
                    TransportResolveClusterAction.REMOTE_TYPE,
                    remoteRequest,
                    ActionListener.releaseAfter(remoteListener, refs.acquire())
                );
            }
        }
    }

    /**
     * Checks the exception against a known list of exceptions that indicate a remote cluster
     * cannot be connected to.
     */
    private boolean notConnectedError(Exception e) {
        if (e instanceof ConnectTransportException || e instanceof NoSuchRemoteClusterException) {
            return true;
        }
        if (e instanceof IllegalStateException && e.getMessage().contains("Unable to open any connections")) {
            return true;
        }
        Throwable ill = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
        if (ill != null && ill.getMessage().contains("unknown host")) {
            return true;
        }
        if (ExceptionsHelper.unwrap(e, NoSeedNodeLeftException.class) != null) {
            return true;
        }
        return false;
    }

    /**
     * Checks whether the local cluster has any matching indices (non-closed), aliases or data streams for
     * the index expression captured in localIndices.
     *
     * @param localIndices
     * @param indicesOptions
     * @param clusterState
     * @return true if indices has at least one matching non-closed index or any aliases or data streams match
     * @throws IndexNotFoundException if a (non-wildcarded) index, alias or data stream is requested
     */
    private boolean hasMatchingIndices(OriginalIndices localIndices, IndicesOptions indicesOptions, ClusterState clusterState) {
        List<ResolveIndexAction.ResolvedIndex> indices = new ArrayList<>();
        List<ResolveIndexAction.ResolvedAlias> aliases = new ArrayList<>();
        List<ResolveIndexAction.ResolvedDataStream> dataStreams = new ArrayList<>();

        // this throws IndexNotFoundException if any (non-wildcard) index in the localIndices list is not present
        ResolveIndexAction.TransportAction.resolveIndices(
            localIndices.indices(),
            indicesOptions,
            clusterState,
            indexNameExpressionResolver,
            indices,
            aliases,
            dataStreams
        );

        /*
         * the _resolve/index action does not track closed status of aliases or data streams,
         * so we can only filter out closed indices here
         */
        return hasNonClosedMatchingIndex(indices) || aliases.size() > 0 || dataStreams.size() > 0;
    }

    static boolean hasNonClosedMatchingIndex(List<ResolveIndexAction.ResolvedIndex> indices) {
        boolean indexMatches = false;
        for (ResolveIndexAction.ResolvedIndex index : indices) {
            String[] attributes = index.getAttributes();
            if (attributes != null) {
                for (String attribute : attributes) {
                    if (attribute.equals("closed")) {
                        indexMatches = false;
                        break;
                    } else {
                        indexMatches = true;
                    }
                }
            }
            if (indexMatches) {
                break;
            }
        }
        return indexMatches;
    }

    private static void releaseResourcesOnCancel(Map<String, ResolveClusterInfo> clusterInfoMap) {
        logger.trace("clear resolve-cluster responses on cancellation");
        clusterInfoMap.clear();
    }
}
