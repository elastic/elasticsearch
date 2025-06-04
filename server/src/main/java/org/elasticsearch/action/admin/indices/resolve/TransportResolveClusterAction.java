/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
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

    public static final String NAME = "indices:admin/resolve/cluster";
    public static final ActionType<ResolveClusterActionResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<ResolveClusterActionResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        ResolveClusterActionResponse::new
    );

    private static final String DUMMY_INDEX_FOR_OLDER_CLUSTERS = "*:dummy*";
    private static final String REMOTE_CONNECTION_TIMEOUT_ERROR = "Request timed out before receiving a response from the remote cluster";

    private final Executor searchCoordinationExecutor;
    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final boolean ccsCheckCompatibility;
    private final ThreadPool threadPool;

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
        this.threadPool = threadPool;
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

        Map<String, ResolveClusterInfo> clusterInfoMap = new ConcurrentHashMap<>();
        Map<String, OriginalIndices> remoteClusterIndices;
        if (request.clusterInfoOnly()) {
            if (request.queryingCluster()) {
                /*
                 * User does not want to check whether an index expression matches, so we use the "*:dummy*" index pattern to
                 * 1) determine all the local configured remote cluster and
                 * 2) for older clusters that do not understand the new clusterInfoOnly setting (or for even older clusters
                 *    where we need to fall back to using _resolve/index), we have to provide an index expression so use dummy*
                 *    and then ignore the matching_indices value that comes back from those remotes. This is preferable to sending
                 *    just "*" since that could be an expensive operation on clusters with thousands of indices/aliases/datastreams
                 */
                String[] dummyIndexExpr = new String[] { DUMMY_INDEX_FOR_OLDER_CLUSTERS };
                remoteClusterIndices = remoteClusterService.groupIndices(IndicesOptions.DEFAULT, dummyIndexExpr, false);
                if (remoteClusterIndices.isEmpty()) {
                    // no remote clusters are configured on the primary "querying" cluster
                    listener.onResponse(new ResolveClusterActionResponse(Map.of()));
                    return;
                }
            } else {
                // on remote if clusterInfoOnly is requested, don't bother with index expression matching
                ResolveClusterInfo resolveClusterInfo = new ResolveClusterInfo(true, false, null, Build.current());
                clusterInfoMap.put(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, resolveClusterInfo);
                listener.onResponse(new ResolveClusterActionResponse(clusterInfoMap));
                return;
            }
        } else {
            remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(), request.indices(), false);
        }

        OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

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
                var remoteRequest = new ResolveClusterActionRequest(
                    originalIndices.indices(),
                    request.indicesOptions(),
                    request.clusterInfoOnly(),
                    false
                );
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
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(info, skipUnavailable, request.clusterInfoOnly()));
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
                        if (ExceptionsHelper.isRemoteUnavailableException((failure))) {
                            String errorMessage = failure.getMessage();
                            /*
                             * If the request timed out, set the error field in the response we send back. This is so that we could
                             * differentiate between connection error to a remote vs. request time out since the "connected" property
                             * cannot provide the additional context in the latter case.
                             */
                            if (errorMessage.equals(REMOTE_CONNECTION_TIMEOUT_ERROR)) {
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable, errorMessage));
                            } else {
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable));
                            }
                        } else if (ExceptionsHelper.unwrap(
                            failure,
                            ElasticsearchSecurityException.class
                        ) instanceof ElasticsearchSecurityException ese) {
                            /*
                             * some ElasticsearchSecurityExceptions come from the local cluster security interceptor after you've
                             * issued the client.execute call but before any call went to the remote cluster, so with an
                             * ElasticsearchSecurityException you can't tell whether the remote cluster is available or not, so mark
                             * it as connected=false
                             */
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(false, skipUnavailable, ese.getMessage()));
                        } else if (ExceptionsHelper.unwrap(failure, IndexNotFoundException.class) instanceof IndexNotFoundException infe) {
                            clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable, infe.getMessage()));
                        } else {
                            Throwable cause = ExceptionsHelper.unwrapCause(failure);
                            // when querying an older cluster that does not have the _resolve/cluster endpoint, we will get
                            // this error at the Transport layer BEFORE it sends the request to the remote cluster, since there
                            // are version guards on the Writeables for this Action, namely ResolveClusterActionRequest.writeTo
                            if (cause instanceof UnsupportedOperationException
                                && cause.getMessage().contains(ResolveClusterActionRequest.TRANSPORT_VERSION_ERROR_MESSAGE_PREFIX)) {
                                // Since this cluster does not have _resolve/cluster, we call the _resolve/index
                                // endpoint to fill in the matching_indices field of the response for that cluster
                                ResolveIndexAction.Request resolveIndexRequest = new ResolveIndexAction.Request(
                                    originalIndices.indices(),
                                    originalIndices.indicesOptions()
                                );
                                ActionListener<ResolveIndexAction.Response> resolveIndexActionListener = createResolveIndexActionListener(
                                    clusterAlias,
                                    request.clusterInfoOnly(),
                                    skipUnavailable,
                                    clusterInfoMap,
                                    resolveClusterTask
                                );
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

                    /**
                     * Create an ActionListener to handle responses from calls when falling back to use the resolve/index
                     * endpoint from older clusters that don't have the resolve/cluster endpoint.
                     */
                    private static ActionListener<ResolveIndexAction.Response> createResolveIndexActionListener(
                        String clusterAlias,
                        boolean clusterInfoOnly,
                        boolean skipUnavailable,
                        Map<String, ResolveClusterInfo> clusterInfoMap,
                        CancellableTask resolveClusterTask
                    ) {
                        return new ActionListener<>() {
                            @Override
                            public void onResponse(ResolveIndexAction.Response response) {
                                if (resolveClusterTask.isCancelled()) {
                                    releaseResourcesOnCancel(clusterInfoMap);
                                    return;
                                }

                                Boolean matchingIndices = null;
                                if (clusterInfoOnly == false) {
                                    matchingIndices = response.getIndices().size() > 0
                                        || response.getAliases().size() > 0
                                        || response.getDataStreams().size() > 0;
                                }
                                clusterInfoMap.put(clusterAlias, new ResolveClusterInfo(true, skipUnavailable, matchingIndices, null));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (resolveClusterTask.isCancelled()) {
                                    releaseResourcesOnCancel(clusterInfoMap);
                                    return;
                                }

                                ResolveClusterInfo resolveClusterInfo;
                                if (ExceptionsHelper.isRemoteUnavailableException((e))) {
                                    resolveClusterInfo = new ResolveClusterInfo(false, skipUnavailable);
                                } else if (ExceptionsHelper.unwrap(
                                    e,
                                    ElasticsearchSecurityException.class
                                ) instanceof ElasticsearchSecurityException ese) {
                                    /*
                                     * some ElasticsearchSecurityExceptions come from the local cluster security interceptor after you've
                                     * issued the client.execute call but before any call went to the remote cluster, so with an
                                     * ElasticsearchSecurityException you can't tell whether the remote cluster is available or not, so mark
                                     * it as connected=false
                                     */
                                    resolveClusterInfo = new ResolveClusterInfo(false, skipUnavailable, ese.getMessage());
                                } else if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) instanceof IndexNotFoundException ie) {
                                    resolveClusterInfo = new ResolveClusterInfo(true, skipUnavailable, ie.getMessage());
                                } else {
                                    // not clear what the error is here, so be safe and mark the cluster as not connected
                                    String errorMessage = ExceptionsHelper.unwrapCause(e).getMessage();
                                    resolveClusterInfo = new ResolveClusterInfo(false, skipUnavailable, errorMessage);
                                    logger.warn(
                                        () -> Strings.format("Failure from _resolve/index lookup against cluster %s: ", clusterAlias),
                                        e
                                    );
                                }
                                clusterInfoMap.put(clusterAlias, resolveClusterInfo);
                            }
                        };
                    }
                };

                ActionListener<ResolveClusterActionResponse> resultsListener;
                TimeValue timeout = request.getTimeout();
                // Wrap the listener with a timeout since a timeout was specified.
                if (timeout != null) {
                    var releaserListener = ActionListener.releaseAfter(remoteListener, refs.acquire());
                    resultsListener = ListenerTimeouts.wrapWithTimeout(
                        threadPool,
                        timeout,
                        searchCoordinationExecutor,
                        releaserListener,
                        ignored -> releaserListener.onFailure(new ConnectTransportException(null, REMOTE_CONNECTION_TIMEOUT_ERROR))
                    );
                } else {
                    resultsListener = ActionListener.releaseAfter(remoteListener, refs.acquire());
                }

                remoteClusterClient.execute(TransportResolveClusterAction.REMOTE_TYPE, remoteRequest, resultsListener);
            }
        }
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
            clusterState.projectState(),
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
