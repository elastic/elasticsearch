/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.RequestDispatcher;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

/**
 * A fork of the field-caps API for ES|QL. This fork allows us to gradually introduce features and optimizations to this internal
 * API without risking breaking the external field-caps API. For now, this API delegates to the field-caps API, but gradually,
 * we will decouple this API completely from the field-caps.
 */
public class EsqlResolveFieldsAction extends HandledTransportAction<FieldCapabilitiesRequest, EsqlResolveFieldsResponse> {
    public static final String NAME = "indices:data/read/esql/resolve_fields";
    public static final ActionType<EsqlResolveFieldsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<FieldCapabilitiesResponse> RESOLVE_REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        FieldCapabilitiesResponse::new
    );
    private static final Logger LOGGER = LogManager.getLogger(EsqlResolveFieldsAction.class);

    private final Executor searchCoordinationExecutor;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final IndicesService indicesService;
    private final boolean ccsCheckCompatibility;
    private final ThreadPool threadPool;
    private final TimeValue forceConnectTimeoutSecs;

    @Inject
    public EsqlResolveFieldsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, FieldCapabilitiesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.searchCoordinationExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
        this.threadPool = threadPool;
        this.forceConnectTimeoutSecs = clusterService.getSettings().getAsTime("search.ccs.force_connect_timeout", null);
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<EsqlResolveFieldsResponse> listener) {
        executeRequest(task, request, listener);
    }

    public void executeRequest(Task task, FieldCapabilitiesRequest request, ActionListener<EsqlResolveFieldsResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        searchCoordinationExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
    }

    private void doExecuteForked(Task task, FieldCapabilitiesRequest request, ActionListener<EsqlResolveFieldsResponse> listener) {
        if (request.isMergeResults()) {
            throw new IllegalArgumentException("merging results not supported");
        }
        if (ccsCheckCompatibility) {
            checkCCSVersionCompatibility(request);
        }
        final Executor singleThreadedExecutor = TransportFieldCapabilitiesAction.buildSingleThreadedExecutor(
            searchCoordinationExecutor,
            LOGGER
        );
        assert task instanceof CancellableTask;
        final CancellableTask fieldCapTask = (CancellableTask) task;
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        ClusterState clusterState = clusterService.state();
        ProjectState projectState = projectResolver.getProjectState(clusterState);
        AtomicReference<TransportVersion> minTransportVersion = new AtomicReference<>(clusterState.getMinTransportVersion());
        final Map<String, OriginalIndices> remoteClusterIndices = transportService.getRemoteClusterService()
            .groupIndices(request.indicesOptions(), request.indices(), request.returnLocalAll());
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(projectState.metadata(), localIndices);
        }

        if (concreteIndices.length == 0 && remoteClusterIndices.isEmpty()) {
            // No indices at all!
            listener.onResponse(
                new EsqlResolveFieldsResponse(
                    new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()),
                    minTransportVersion.get()
                )
            );
            return;
        }

        TransportFieldCapabilitiesAction.checkIndexBlocks(projectState, concreteIndices);
        final TransportFieldCapabilitiesAction.FailureCollector indexFailures = new TransportFieldCapabilitiesAction.FailureCollector();
        final Map<String, FieldCapabilitiesIndexResponse> indexResponses = new HashMap<>();
        // This map is used to share the index response for indices which have the same index mapping hash to reduce the memory usage.
        final Map<String, FieldCapabilitiesIndexResponse> indexMappingHashToResponses = new HashMap<>();
        final Runnable releaseResourcesOnCancel = () -> {
            LOGGER.trace("clear index responses on cancellation");
            indexFailures.clear();
            indexResponses.clear();
            indexMappingHashToResponses.clear();
        };
        final Consumer<FieldCapabilitiesIndexResponse> handleIndexResponse = resp -> {
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
                return;
            }
            if (resp.canMatch() && resp.getIndexMappingHash() != null) {
                FieldCapabilitiesIndexResponse curr = indexMappingHashToResponses.putIfAbsent(resp.getIndexMappingHash(), resp);
                if (curr != null) {
                    resp = new FieldCapabilitiesIndexResponse(
                        resp.getIndexName(),
                        curr.getIndexMappingHash(),
                        curr.get(),
                        true,
                        curr.getIndexMode()
                    );
                }
            }
            if (request.includeEmptyFields()) {
                indexResponses.putIfAbsent(resp.getIndexName(), resp);
            } else {
                indexResponses.merge(resp.getIndexName(), resp, (a, b) -> {
                    if (a.get().equals(b.get())) {
                        return a;
                    }
                    Map<String, IndexFieldCapabilities> mergedCaps = new HashMap<>(a.get());
                    mergedCaps.putAll(b.get());
                    return new FieldCapabilitiesIndexResponse(
                        a.getIndexName(),
                        a.getIndexMappingHash(),
                        mergedCaps,
                        true,
                        a.getIndexMode()
                    );
                });
            }
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
            }
        };
        final BiConsumer<String, Exception> handleIndexFailure = (index, error) -> {
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
                return;
            }
            indexFailures.collect(index, error);
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
            }
        };
        final var finishedOrCancelled = new AtomicBoolean();
        fieldCapTask.addListener(() -> {
            if (finishedOrCancelled.compareAndSet(false, true)) {
                singleThreadedExecutor.execute(releaseResourcesOnCancel);
                LOGGER.trace("clear index responses on cancellation submitted");
            }
        });
        try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
            finishedOrCancelled.set(true);
            if (fieldCapTask.notifyIfCancelled(listener)) {
                releaseResourcesOnCancel.run();
            } else {
                finishHim(
                    indexResponses,
                    indexFailures,
                    listener.map(caps -> new EsqlResolveFieldsResponse(caps, minTransportVersion.get()))
                );
            }
        })) {
            // local cluster
            final RequestDispatcher requestDispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                projectResolver,
                indicesService.getCoordinatorRewriteContextProvider(() -> nowInMillis),
                task,
                request,
                localIndices,
                nowInMillis,
                concreteIndices,
                singleThreadedExecutor,
                handleIndexResponse,
                handleIndexFailure,
                refs.acquire()::close
            );
            requestDispatcher.execute();

            // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
            // send us back all individual index results.
            for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                String clusterAlias = remoteIndices.getKey();
                OriginalIndices originalIndices = remoteIndices.getValue();
                FieldCapabilitiesRequest remoteRequest = TransportFieldCapabilitiesAction.prepareRemoteRequest(
                    clusterAlias,
                    request,
                    originalIndices,
                    nowInMillis
                );
                ActionListener<EsqlResolveFieldsResponse> remoteListener = ActionListener.wrap(response -> {
                    for (FieldCapabilitiesIndexResponse resp : response.caps().getIndexResponses()) {
                        String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, resp.getIndexName());
                        handleIndexResponse.accept(
                            new FieldCapabilitiesIndexResponse(
                                indexName,
                                resp.getIndexMappingHash(),
                                resp.get(),
                                resp.canMatch(),
                                resp.getIndexMode()
                            )
                        );
                    }
                    for (FieldCapabilitiesFailure failure : response.caps().getFailures()) {
                        Exception ex = failure.getException();
                        for (String index : failure.getIndices()) {
                            handleIndexFailure.accept(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index), ex);
                        }
                    }
                    minTransportVersion.accumulateAndGet(response.minTransportVersion(), (lhs, rhs) -> {
                        if (lhs == null || rhs == null) {
                            return null;
                        }
                        return TransportVersion.min(lhs, rhs);
                    });
                }, ex -> {
                    for (String index : originalIndices.indices()) {
                        handleIndexFailure.accept(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index), ex);
                    }
                });

                SubscribableListener<Transport.Connection> connectionListener = new SubscribableListener<>();
                if (forceConnectTimeoutSecs != null) {
                    connectionListener.addTimeout(forceConnectTimeoutSecs, threadPool, singleThreadedExecutor);
                }

                connectionListener.addListener(
                    // The underlying transport service may call onFailure with a thread pool other than search_coordinator.
                    // This fork is a workaround to ensure that the merging of field-caps always occurs on the search_coordinator.
                    // TODO: remove this workaround after we fixed https://github.com/elastic/elasticsearch/issues/107439
                    new TransportFieldCapabilitiesAction.ForkingOnFailureActionListener<>(
                        singleThreadedExecutor,
                        true,
                        ActionListener.releaseAfter(remoteListener, refs.acquire())
                    ).delegateFailure(
                        (responseListener, conn) -> transportService.sendRequest(
                            conn,
                            RESOLVE_REMOTE_TYPE.name(),
                            remoteRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(responseListener, EsqlResolveFieldsResponse::new, singleThreadedExecutor)
                        )
                    )
                );

                boolean ensureConnected = forceConnectTimeoutSecs != null
                    || transportService.getRemoteClusterService().isSkipUnavailable(clusterAlias).orElse(true) == false;
                transportService.getRemoteClusterService()
                    .maybeEnsureConnectedAndGetConnection(clusterAlias, ensureConnected, connectionListener);
            }
        }
    }

    private static void finishHim(
        Map<String, FieldCapabilitiesIndexResponse> indexResponses,
        TransportFieldCapabilitiesAction.FailureCollector indexFailures,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        List<FieldCapabilitiesFailure> failures = indexFailures.build(indexResponses.keySet());
        if (indexResponses.isEmpty() == false) {
            listener.onResponse(new FieldCapabilitiesResponse(new ArrayList<>(indexResponses.values()), failures));
        } else {
            // we have no responses at all, maybe because of errors
            if (indexFailures.isEmpty() == false) {
                /*
                 * Under no circumstances are we to pass timeout errors originating from SubscribableListener as top-level errors.
                 * Instead, they should always be passed through the response object, as part of "failures".
                 */
                if (failures.stream()
                    .anyMatch(
                        failure -> failure.getException() instanceof IllegalStateException ise
                            && ise.getCause() instanceof ElasticsearchTimeoutException
                    )) {
                    listener.onResponse(new FieldCapabilitiesResponse(Collections.emptyList(), failures));
                } else {
                    // throw back the first exception
                    listener.onFailure(failures.get(0).getException());
                }
            } else {
                listener.onResponse(new FieldCapabilitiesResponse(Collections.emptyList(), Collections.emptyList()));
            }
        }
    }
}
