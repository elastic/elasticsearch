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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
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

    public static final String NAME = "indices:admin/resolve/cluster";
    public static final ActionType<ResolveClusterActionResponse> TYPE = new ActionType<>(NAME, ResolveClusterActionResponse::new);
    public static final String ACTION_NODE_NAME = NAME + "[n]"; // TODO: do we need this?

    private final ThreadPool threadPool;
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
        this.threadPool = threadPool;
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
        if (remoteClusterIndices.size() > 0) {
            final Runnable releaseResourcesOnCancel = () -> {
                logger.trace("clear index responses on cancellation");
                clusterInfoMap.clear();
            };

            // add local cluster info if in scope of the index-expression from user
            if (localIndices != null) {
                clusterInfoMap.put(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    new ResolveClusterInfo(true, false, hasMatchingIndices(localIndices, clusterState), Build.current())
                );
            }

            final var finishedOrCancelled = new AtomicBoolean();
            resolveClusterTask.addListener(() -> {
                if (finishedOrCancelled.compareAndSet(false, true)) {
                    releaseResourcesOnCancel.run();
                }
            });

            try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
                finishedOrCancelled.set(true);
                if (resolveClusterTask.notifyIfCancelled(listener)) {
                    releaseResourcesOnCancel.run();
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
                    Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                        threadPool,
                        clusterAlias,
                        searchCoordinationExecutor
                    );
                    ResolveClusterActionRequest remoteRequest = new ResolveClusterActionRequest(originalIndices.indices());
                    ActionListener<ResolveClusterActionResponse> remoteListener = new ActionListener<ResolveClusterActionResponse>() {
                        @Override
                        public void onResponse(ResolveClusterActionResponse response) {
                            if (resolveClusterTask.isCancelled()) {
                                releaseResourcesOnCancel.run();
                                return;
                            }
                            ResolveClusterInfo info = response.getResolveClusterInfo().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                            if (info != null) {
                                clusterInfoMap.put(
                                    clusterAlias,
                                    new ResolveClusterInfo(info.isConnected(), skipUnavailable, info.getMatchingIndices(), info.getBuild())
                                );
                            }
                            if (resolveClusterTask.isCancelled()) {
                                releaseResourcesOnCancel.run();
                            }
                        }

                        @Override
                        public void onFailure(Exception failure) {
                            if (resolveClusterTask.isCancelled()) {
                                releaseResourcesOnCancel.run();
                                return;
                            }
                            if (notConnectedError(failure)) {
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable));
                            } else if (clusterResolveEndpointNotFound(failure)) {
                                // if the endpoint returns an error that it does not _resolve/cluster, we know we are connected
                                // TODO: call remoteClusterClient.admin().indices().resolveIndex() to fill in 'matching_indices'?
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable));
                            } else {
                                Throwable cause = ExceptionsHelper.unwrapCause(failure);
                                // it is not clear that this error indicates that the cluster is disconnected, but it is hard to
                                // determine based on the error, so we default to false in the face of any error and report it
                                // back to the user for consideration
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable, cause.toString()));
                                logger.warn(
                                    () -> Strings.format("Failure from _resolve/cluster lookup against cluster %s: ", clusterAlias),
                                    failure
                                );
                            }
                            if (resolveClusterTask.isCancelled()) {
                                releaseResourcesOnCancel.run();
                            }
                        }
                    };
                    remoteClusterClient.admin()
                        .indices()
                        .execute(
                            TransportResolveClusterAction.TYPE,
                            remoteRequest,
                            ActionListener.releaseAfter(remoteListener, refs.acquire())
                        );
                }
            }
        } else {
            // executes on remote cluster (or local cluster if no remote clusters were specified in the user's index expression)
            resolveClusterTask.ensureNotCancelled();
            clusterInfoMap.put(
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                new ResolveClusterInfo(true, null, hasMatchingIndices(localIndices, clusterState), Build.current())
            );
            listener.onResponse(new ResolveClusterActionResponse(clusterInfoMap));
        }
    }

    private boolean clusterResolveEndpointNotFound(Exception failure) {
        // if the _cluster/resolve endpoint is not present on the remote cluster, the error returned will be:
        // org.elasticsearch.transport.RemoteTransportException: [<host>][<ip>:<port>][indices:admin/resolve/cluster]
        // Cause: org.elasticsearch.transport.ActionNotFoundTransportException: No handler for action [indices:admin/resolve/cluster]
        if (failure instanceof RemoteTransportException) {
            return ExceptionsHelper.unwrap(failure, ActionNotFoundTransportException.class) != null;
        }
        return false;
    }

    /**
     * Checks the exception against a known list of exceptions that indicate a remote cluster
     * cannot be connected to.
     */
    private boolean notConnectedError(Exception e) {
        if (e instanceof ConnectTransportException || e instanceof NoSuchRemoteClusterException) {
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

    private boolean hasMatchingIndices(OriginalIndices localIndices, ClusterState clusterState) {
        List<ResolveIndexAction.ResolvedIndex> indices = new ArrayList<>();
        List<ResolveIndexAction.ResolvedAlias> aliases = new ArrayList<>();
        List<ResolveIndexAction.ResolvedDataStream> dataStreams = new ArrayList<>();
        try {
            ResolveIndexAction.TransportAction.resolveIndices(
                localIndices,
                clusterState,
                indexNameExpressionResolver,
                indices,
                aliases,
                dataStreams
            );

        } catch (IndexNotFoundException e) {
            return false;
        }

        return indices.size() > 0 || aliases.size() > 0 || dataStreams.size() > 0;
    }
}
