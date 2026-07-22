/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.RemoteDatasetNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteResourceNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteViewNotSupportedException;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.view.ViewResolutionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A fork of the field-caps API for ES|QL. This fork allows us to gradually introduce features and optimizations to this internal
 * API without risking breaking the external field-caps API. For now, this API delegates to the field-caps API, but gradually,
 * we will decouple this API completely from the field-caps.
 */
public class EsqlResolveFieldsAction extends HandledTransportAction<FieldCapabilitiesRequest, EsqlResolveFieldsResponse> {
    public static final String NAME = "indices:data/read/esql/resolve_fields";
    public static final ActionType<EsqlResolveFieldsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<EsqlResolveFieldsResponse> RESOLVE_REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        EsqlResolveFieldsResponse::new
    );

    private final TransportFieldCapabilitiesAction fieldCapsAction;
    private final ClusterService clusterService;
    private final ViewResolutionService viewResolutionService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ProjectResolver projectResolver;

    @Inject
    public EsqlResolveFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        TransportFieldCapabilitiesAction fieldCapsAction,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, FieldCapabilitiesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.fieldCapsAction = fieldCapsAction;
        this.clusterService = clusterService;
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<EsqlResolveFieldsResponse> listener) {
        // resolveViews / resolveDatasets are only set on a request from the originating cluster, so this detection runs
        // only on a remote cluster. Views and datasets are both non-remotable abstractions; detect both here and report
        // them together, so a single remote that hosts both fails with one exception naming both rather than just the
        // first kind checked.
        var abstractionOptions = request.indicesOptions().indexAbstractionOptions();
        List<String> remoteViews = abstractionOptions.resolveViews()
            ? qualify(request.clusterAlias(), getViews(request.indices(), request.indicesOptions(), request.getResolvedIndexExpressions()))
            : List.of();
        // When federation is suppressed this node reports no datasets, so a FROM <remote:name> falls through to normal
        // remote index resolution and the node is indistinguishable from one that never shipped the feature, rather than
        // failing with a RemoteDatasetNotSupportedException that names pre-existing datasets still in cluster state.
        List<String> remoteDatasets = abstractionOptions.resolveDatasets() && Federation.isAvailable()
            ? qualify(request.clusterAlias(), getDatasets(request.indices(), request.indicesOptions()))
            : List.of();
        boolean hasRemoteViews = remoteViews.isEmpty() == false;
        boolean hasRemoteDatasets = remoteDatasets.isEmpty() == false;
        if (hasRemoteViews || hasRemoteDatasets) {
            // A coordinator that asked for datasets (resolveDatasets) also understands the combined exception; an older,
            // views-only coordinator only knows RemoteViewNotSupportedException, so a single-kind failure keeps using the
            // per-kind exception it can deserialize.
            ElasticsearchException failure;
            if (hasRemoteViews && hasRemoteDatasets) {
                failure = new RemoteResourceNotSupportedException(remoteViews, remoteDatasets);
            } else if (hasRemoteViews) {
                failure = new RemoteViewNotSupportedException(remoteViews);
            } else {
                failure = new RemoteDatasetNotSupportedException(remoteDatasets);
            }
            listener.onFailure(failure);
            return;
        }

        fieldCapsAction.executeRequest(task, request, new TransportFieldCapabilitiesAction.LinkedRequestExecutor<>() {
            @Override
            public void executeRemoteRequest(
                TransportService transportService,
                Transport.Connection conn,
                FieldCapabilitiesRequest remoteRequest,
                ActionListenerResponseHandler<FieldCapabilitiesResponse> responseHandler
            ) {
                remoteRequest.indicesOptions(
                    IndicesOptions.builder(remoteRequest.indicesOptions())
                        .indexAbstractionOptions(
                            IndicesOptions.IndexAbstractionOptions.builder(remoteRequest.indicesOptions().indexAbstractionOptions())
                                .resolveViews(true)
                                .resolveDatasets(true)
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
}
