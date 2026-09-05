/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.RemoteDatasetNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteResourceNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteViewNotSupportedException;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.view.ViewResolutionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;

/**
 * A fork of the field-caps API for ES|QL. This fork allows us to gradually introduce features and optimizations to this internal
 * API without risking breaking the external field-caps API. For now, this API delegates to the field-caps API, but gradually,
 * we will decouple this API completely from the field-caps.
 */
public class EsqlResolveFieldsAction extends HandledTransportAction<EsqlResolveFieldsRequest, EsqlResolveFieldsResponse> {
    public static final String NAME = "indices:data/read/esql/resolve_fields";
    public static final ActionType<EsqlResolveFieldsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<EsqlResolveFieldsResponse> RESOLVE_REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        EsqlResolveFieldsResponse::new
    );

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexAbstractionResolver indexAbstractionResolver;
    private final NodeClient client;
    private final Executor searchCoordinationExecutor;

    private final TransportFieldCapabilitiesAction fieldCapsAction;

    private final ViewResolutionService viewResolutionService;
    private final boolean federationAvailable;

    @Inject
    public EsqlResolveFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        CrossProjectModeDecider crossProjectModeDecider,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client,
        ThreadPool threadPool,
        TransportFieldCapabilitiesAction fieldCapsAction
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, EsqlResolveFieldsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.crossProjectModeDecider = crossProjectModeDecider;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        this.client = client;
        this.searchCoordinationExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        this.fieldCapsAction = fieldCapsAction;

        // TODO cleanup
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
        this.federationAvailable = Federation.isAvailable(clusterService.getSettings());
    }

    @Override
    protected void doExecute(Task task, EsqlResolveFieldsRequest request, final ActionListener<EsqlResolveFieldsResponse> listener) {
        // doResolveWithFieldCaps(task, request, listener);
        searchCoordinationExecutor.execute(ActionRunnable.wrap(listener, l -> doResolve(task, request, l)));
    }

    private void doResolve(Task task, EsqlResolveFieldsRequest request, ActionListener<EsqlResolveFieldsResponse> listener) {

        long nowInMillis = Objects.requireNonNullElseGet(request.fieldCapsRequest().nowInMillis(), System::currentTimeMillis);

        final ProjectState projectState = projectResolver.getProjectState(clusterService.state());

        // TODO request filter
        // TODO check task cancellation
        // TODO validate CPS resolution correctness

        final boolean resolveCrossProject = crossProjectModeDecider.resolvesCrossProject(request);
        final IndicesOptions indicesOptions = prepareIndicesOptions(request.indicesOptions(), resolveCrossProject);

        final Map<String, OriginalIndices> remoteIndices = transportService.getRemoteClusterService()
            .groupIndices(indicesOptions, request.indices(), false);
        final OriginalIndices localIndices = remoteIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        var response = new EsqlResolveFieldsResponseBuilder(clusterService.state().getMinTransportVersion());
        try (
            var resultListener = new RefCountingListener(
                new ThreadedActionListener<>(
                    searchCoordinationExecutor,
                    listener.delegateFailureAndWrap((l, r) -> l.onResponse(response.build()))
                )
            )
        ) {
            try {
                // local resolutions
                if (localIndices != null) {
                    var abstractions = resolveIndexAbstractions(localIndices, projectState);

                    if (Strings.isNotBlank(request.fieldCapsRequest().clusterAlias())) {
                        // validate no remote resources
                        var views = qualifyIndexAbstraction(request.fieldCapsRequest().clusterAlias(), abstractions.views);
                        var datasets = qualifyIndexAbstraction(request.fieldCapsRequest().clusterAlias(), abstractions.datasets);

                        if (!views.isEmpty() && !datasets.isEmpty()) {
                            throw new RemoteResourceNotSupportedException(views, datasets);
                        } else if (!views.isEmpty()) {
                            throw new RemoteViewNotSupportedException(views);
                        } else if (!datasets.isEmpty()) {
                            throw new RemoteDatasetNotSupportedException(datasets);
                        }
                    }

                    response.resolvedLocally = abstractions.expressions;
                    resolveConcreteIndices(request, abstractions.indices, response, resultListener);
                    // TODO resolve views fields
                    // TODO resolve datasets fields
                }

                // remote resolutions
                for (var entry : remoteIndices.entrySet()) {
                    String clusterAlias = entry.getKey();
                    OriginalIndices indices = entry.getValue();

                    // TODO connection timeout
                    // TODO can match
                    boolean ensureConnected = transportService.getRemoteClusterService()
                        .isSkipUnavailable(clusterAlias)
                        .orElse(true) == false;
                    var forkedListener = resultListener.acquire();
                    SubscribableListener.<Transport.Connection>newForked(
                        l -> transportService.getRemoteClusterService()
                            .maybeEnsureConnectedAndGetConnection(clusterAlias, ensureConnected, l)
                    ).<EsqlResolveFieldsResponse>andThen((l, connection) -> {
                        var remoteRequest = new EsqlResolveFieldsRequest(
                            TransportFieldCapabilitiesAction.prepareRemoteRequest(
                                clusterAlias,
                                request.fieldCapsRequest(),
                                indices,
                                nowInMillis,
                                resolveCrossProject
                            )
                        );
                        remoteRequest.fieldCapsRequest().indicesOptions(indicesOptions);
                        assert remoteRequest.fieldCapsRequest().indicesOptions().indexAbstractionOptions().resolveViews() : "wtf";
                        transportService.sendRequest(
                            connection,
                            NAME,
                            remoteRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(l, EsqlResolveFieldsResponse::new, searchCoordinationExecutor)
                        );
                    })
                        .addListener(
                            ActionListener.runAfter(
                                ActionListener.wrap(
                                    remoteResponse -> response.appendRemoteResponse(clusterAlias, remoteResponse),
                                    remoteException -> response.addRemoteException(clusterAlias, indices.indices(), remoteException)
                                ),
                                () -> forkedListener.onResponse(null)
                            )
                        );
                }
            } catch (Exception e) {
                logger.warn("Unexpected error while resolving fields", e);
                resultListener.acquire().onFailure(e);
            }
        }
    }

    private IndicesOptions prepareIndicesOptions(IndicesOptions indicesOptions, boolean resolveCrossProject) {
        // indicesOptions = IndicesOptions.builder(indicesOptions)
        // .indexAbstractionOptions(
        // // TODO make configurable depending on Federation.isAvailable(clusterService.getSettings())
        // IndicesOptions.IndexAbstractionOptions.builder().resolveAliases(true).resolveViews(true).resolveDatasets(true).build()
        // )
        // .build();
        return resolveCrossProject ? indicesOptionsForCrossProjectFanout(indicesOptions) : indicesOptions;
    }

    private ResolvedIndexAbstractions resolveIndexAbstractions(OriginalIndices localIndices, ProjectState projectState) {
        var indicesLookup = projectState.metadata().getIndicesLookup();
        var expressions = indexAbstractionResolver.resolveIndexAbstractions(
            List.of(localIndices.indices()),
            localIndices.indicesOptions(),
            projectState.metadata(),
            componentSelector -> indicesLookup.keySet(),
            (index, selector) -> true,
            true
        );

        var result = new ResolvedIndexAbstractions(expressions);

        for (var expression : expressions.expressions()) {
            if (expression.localExpressions().localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS) {
                for (var index : expression.localExpressions().indices()) {
                    var nameAndSelector = IndexNameExpressionResolver.splitSelectorExpression(index);
                    var indexAbstraction = indicesLookup.get(nameAndSelector.v1());
                    if (indexAbstraction != null) {
                        // TODO should we allow selectors on non-data streams?
                        switch (indexAbstraction.getType()) {
                            case CONCRETE_INDEX -> result.indices.add(indexAbstraction);
                            // Pass aliases by name, security authorizes at alias level,
                            case ALIAS -> result.indices.add(indexAbstraction);
                            case DATA_STREAM -> {
                                List<Index> source = switch (IndexComponentSelector.getByKey(nameAndSelector.v2())) {
                                    case null -> indexAbstraction.getIndices();
                                    case DATA -> indexAbstraction.getIndices();
                                    case FAILURES -> indexAbstraction.getFailureIndices(projectState.metadata());
                                };
                                source.stream()
                                    .map(target -> indicesLookup.get(target.getName()))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toCollection(() -> result.indices));
                            }
                            case VIEW -> result.views.add(indexAbstraction);
                            case DATASET -> result.datasets.add(indexAbstraction);
                        }
                    }
                }
            }
        }
        return result;
    }

    private void resolveConcreteIndices(
        EsqlResolveFieldsRequest request,
        List<IndexAbstraction> indices,
        EsqlResolveFieldsResponseBuilder builder,
        RefCountingListener listener
    ) {
        if (indices.isEmpty()) {
            return;
        }

        var concreteIndices = indices.stream().map(IndexAbstraction::getName).toArray(String[]::new);

        FieldCapabilitiesRequest fcRequest = new FieldCapabilitiesRequest();
        fcRequest.indices(concreteIndices);
        fcRequest.fields(request.fieldCapsRequest().fields());
        fcRequest.includeUnmapped(true);
        fcRequest.indexFilter(request.fieldCapsRequest().indexFilter());
        fcRequest.returnLocalAll(false);
        // lenient because we throw our own errors looking at the response e.g. if something was not resolved
        // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
        fcRequest.indicesOptions(request.indicesOptions());
        // we ignore the nested data type fields starting with https://github.com/elastic/elasticsearch/pull/111495
        fcRequest.filters(request.fieldCapsRequest().filters());
        fcRequest.setMergeResults(false);
        // fcRequest.includeResolvedTo(false);// TODO do we need this?
        // fcRequest.projectRouting(projectRouting);

        client.execute(TransportFieldCapabilitiesAction.TYPE, fcRequest, listener.acquire().delegateFailureAndWrap((l, fcResponse) -> {
            builder.indexResponses.addAll(fcResponse.getIndexResponses());
            builder.failures.addAll(fcResponse.getFailures());
            l.onResponse(null);
        }));
    }

    private static class ResolvedIndexAbstractions {
        private final ResolvedIndexExpressions expressions;
        private final List<IndexAbstraction> indices = new ArrayList<>();
        private final List<IndexAbstraction> views = new ArrayList<>();
        private final List<IndexAbstraction> datasets = new ArrayList<>();

        ResolvedIndexAbstractions(ResolvedIndexExpressions expressions) {
            this.expressions = expressions;
        }
    }

    private static class EsqlResolveFieldsResponseBuilder {

        private TransportVersion minTransportVersion;
        private ResolvedIndexExpressions resolvedLocally;
        private final Map<String, ResolvedIndexExpressions> resolvedRemotely = new HashMap<>();
        private final List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
        private final List<FieldCapabilitiesFailure> failures = new ArrayList<>();
        private final List<String> viewsNotFound = new ArrayList<>();
        private final List<String> datasetsNotFound = new ArrayList<>();

        EsqlResolveFieldsResponseBuilder(TransportVersion minTransportVersion) {
            this.minTransportVersion = minTransportVersion;
        }

        void appendRemoteResponse(String clusterAlias, EsqlResolveFieldsResponse remoteResponse) {
            var remoteTransportVersion = remoteResponse.caps().minTransportVersion();
            if (remoteTransportVersion != null) {
                minTransportVersion = TransportVersion.min(minTransportVersion, remoteTransportVersion);
            }
            resolvedRemotely.put(clusterAlias, remoteResponse.caps().getResolvedLocally());
            for (FieldCapabilitiesIndexResponse index : remoteResponse.caps().getIndexResponses()) {
                // TODO deduplicate
                indexResponses.add(
                    new FieldCapabilitiesIndexResponse(
                        RemoteClusterAware.buildRemoteIndexName(clusterAlias, index.getIndexName()),
                        index.getIndexMappingHash(),
                        index.get(),
                        index.canMatch(),
                        index.getIndexMode()
                    )
                );
            }
            // TODO collect failures: reuse FailureCollector?
            failures.addAll(remoteResponse.caps().getFailures());
        }

        void addRemoteException(String clusterAlias, String[] indices, Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof RemoteResourceNotSupportedException resourceException) {
                viewsNotFound.addAll(resourceException.views());
                datasetsNotFound.addAll(resourceException.datasets());
            } else if (cause instanceof RemoteViewNotSupportedException viewException) {
                viewsNotFound.addAll(viewException.views());
            } else if (cause instanceof RemoteDatasetNotSupportedException datasetException) {
                datasetsNotFound.addAll(datasetException.datasets());
            }
            failures.add(
                new FieldCapabilitiesFailure(
                    Arrays.stream(indices).map(i -> RemoteClusterAware.buildRemoteIndexName(clusterAlias, i)).toArray(String[]::new),
                    e
                )
            );
        }

        EsqlResolveFieldsResponse build() {
            if (viewsNotFound.isEmpty() == false && datasetsNotFound.isEmpty() == false) {
                throw new RemoteResourceNotSupportedException(viewsNotFound, datasetsNotFound);
            } else if (viewsNotFound.isEmpty() == false) {
                throw new RemoteViewNotSupportedException(viewsNotFound);
            } else if (datasetsNotFound.isEmpty() == false) {
                throw new RemoteDatasetNotSupportedException(datasetsNotFound);
            }
            return new EsqlResolveFieldsResponse(
                FieldCapabilitiesResponse.builder()
                    .withMinTransportVersion(minTransportVersion)
                    .withResolvedLocally(resolvedLocally)
                    .withResolvedRemotely(resolvedRemotely)
                    .withIndexResponses(indexResponses)
                    .withFailures(failures)
                    .build()
            );
        }
    }

    private void doResolveWithFieldCaps(Task task, EsqlResolveFieldsRequest request, ActionListener<EsqlResolveFieldsResponse> listener) {
        var failure = validateNoRemoteViewsOrDatasets(request);
        if (failure != null) {
            listener.onFailure(failure);
            return;
        }

        fieldCapsAction.executeRequest(task, request.fieldCapsRequest(), new TransportFieldCapabilitiesAction.LinkedRequestExecutor<>() {
            @Override
            public void executeRemoteRequest(
                TransportService transportService,
                Transport.Connection conn,
                FieldCapabilitiesRequest remoteRequest,
                ActionListenerResponseHandler<FieldCapabilitiesResponse> responseHandler
            ) {
                // A node without federation does not ask its remotes for datasets either, so a remote that has the feature on
                // cannot make FROM <remote>:<name> fail here with an error naming datasets; the name falls through to normal
                // remote index resolution instead.
                remoteRequest.indicesOptions(
                    IndicesOptions.builder(remoteRequest.indicesOptions())
                        .indexAbstractionOptions(
                            IndicesOptions.IndexAbstractionOptions.builder(remoteRequest.indicesOptions().indexAbstractionOptions())
                                .resolveViews(true)
                                .resolveDatasets(federationAvailable)
                        )
                        .build()
                );
                transportService.sendRequest(
                    conn,
                    RESOLVE_REMOTE_TYPE.name(),
                    remoteRequest,
                    TransportRequestOptions.EMPTY,
                    responseHandler
                );
            }

            @Override
            public EsqlResolveFieldsResponse read(StreamInput in) throws IOException {
                return new EsqlResolveFieldsResponse(in);
            }

            @Override
            public EsqlResolveFieldsResponse wrapPrimary(FieldCapabilitiesResponse primary) {
                return new EsqlResolveFieldsResponse(primary);
            }

            @Override
            public FieldCapabilitiesResponse unwrapPrimary(EsqlResolveFieldsResponse esqlResolveFieldsResponse) {
                return esqlResolveFieldsResponse.caps();
            }
        }, listener);
    }

    private ElasticsearchException validateNoRemoteViewsOrDatasets(EsqlResolveFieldsRequest request) {
        // resolveViews / resolveDatasets are only set on a request from the originating cluster, so this detection runs
        // only on a remote cluster. Views and datasets are both non-remotable abstractions; detect both here and report
        // them together, so a single remote that hosts both fails with one exception naming both rather than just the
        // first kind checked.
        var abstractionOptions = request.indicesOptions().indexAbstractionOptions();
        List<String> remoteViews = abstractionOptions.resolveViews()
            ? qualify(
                request.fieldCapsRequest().clusterAlias(),
                getViews(request.indices(), request.indicesOptions(), request.getResolvedIndexExpressions())
            )
            : List.of();
        // When federation is not available this node reports no datasets, so a FROM <remote:name> falls through to normal
        // remote index resolution and the node is indistinguishable from one that never shipped the feature, rather than
        // failing with a RemoteDatasetNotSupportedException that names pre-existing datasets still in cluster state.
        List<String> remoteDatasets = abstractionOptions.resolveDatasets() && federationAvailable
            ? qualify(request.fieldCapsRequest().clusterAlias(), getDatasets(request.indices(), request.indicesOptions()))
            : List.of();
        boolean hasRemoteViews = remoteViews.isEmpty() == false;
        boolean hasRemoteDatasets = remoteDatasets.isEmpty() == false;

        if (hasRemoteViews || hasRemoteDatasets) {
            // A coordinator that asked for datasets (resolveDatasets) also understands the combined exception; an older,
            // views-only coordinator only knows RemoteViewNotSupportedException, so a single-kind failure keeps using the
            // per-kind exception it can deserialize.
            if (hasRemoteViews && hasRemoteDatasets) {
                return new RemoteResourceNotSupportedException(remoteViews, remoteDatasets);
            } else if (hasRemoteViews) {
                return new RemoteViewNotSupportedException(remoteViews);
            } else {
                return new RemoteDatasetNotSupportedException(remoteDatasets);
            }
        }
        return null;
    }

    private Set<String> getViews(String[] indices, IndicesOptions indicesOptions, ResolvedIndexExpressions resolvedIndexExpressions) {
        var projectState = projectResolver.getProjectState(clusterService.state());
        var result = viewResolutionService.resolveViews(projectState, indices, indicesOptions, resolvedIndexExpressions);
        return Arrays.stream(result.views()).map(View::getName).collect(Collectors.toSet());
    }

    /**
     * Qualify each local abstraction name with the remote cluster alias (sorted for a stable error message).
     */
    private static List<String> qualify(String clusterAlias, Set<String> names) {
        return names.stream().sorted().map(name -> clusterAlias + ":" + name).toList();
    }

    private Set<String> getDatasets(String[] indices, IndicesOptions indicesOptions) {
        // Datasets resolve via IndexNameExpressionResolver, not the view service.
        var projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
        return Set.copyOf(indexNameExpressionResolver.datasets(projectMetadata, indicesOptions, new IndicesRequest() {
            @Override
            public String[] indices() {
                return indices;
            }

            @Override
            public IndicesOptions indicesOptions() {
                return indicesOptions;
            }
        }));
    }

    private static List<String> qualifyIndexAbstraction(String clusterAlias, Collection<IndexAbstraction> names) {
        return names.stream()
            .sorted(Comparator.comparing(IndexAbstraction::getName))
            .map(name -> clusterAlias + ":" + name.getName())
            .toList();
    }
}
