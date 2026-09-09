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
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
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
public class EsqlResolveFieldsAction extends HandledTransportAction<EsqlResolveFieldsRequest, EsqlResolveFieldsResponse> {
    public static final String NAME = "indices:data/read/esql/resolve_fields";
    public static final ActionType<EsqlResolveFieldsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<EsqlResolveFieldsResponse> RESOLVE_REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        EsqlResolveFieldsResponse::new
    );

    private final TransportFieldCapabilitiesAction fieldCapsAction;
    private final ClusterService clusterService;
    private final ViewResolutionService viewResolutionService;
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
        super(NAME, transportService, actionFilters, EsqlResolveFieldsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.fieldCapsAction = fieldCapsAction;
        this.clusterService = clusterService;
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, EsqlResolveFieldsRequest request, final ActionListener<EsqlResolveFieldsResponse> listener) {
        var failure = validateNoRemoteViews(request);
        if (failure != null) {
            listener.onFailure(failure);
            return;
        }

        // A dataset is a registration on the cluster that holds it, read by that cluster's own query, so it must not
        // resolve for a caller on another one. A coordinator that predates that rule still asks for datasets here, and
        // by the time this runs the security layer has already resolved the request under that flag
        // (EsqlResolveFieldsRequest is IndicesRequest.Replaceable, and IndicesAndAliasesResolver reads resolveDatasets),
        // so a dataset name can already be sitting in indices(). Clearing the option stops field-caps resolving it; the
        // lenient ALLOW_UNAVAILABLE_TARGETS that ES|QL resolution uses is then what drops the name rather than failing
        // on it.
        FieldCapabilitiesRequest fieldCapsRequest = request.fieldCapsRequest();
        fieldCapsRequest.indicesOptions(
            IndicesOptions.builder(fieldCapsRequest.indicesOptions())
                .indexAbstractionOptions(
                    IndicesOptions.IndexAbstractionOptions.builder(fieldCapsRequest.indicesOptions().indexAbstractionOptions())
                        .resolveDatasets(false)
                )
                .build()
        );

        fieldCapsAction.executeRequest(task, fieldCapsRequest, new TransportFieldCapabilitiesAction.LinkedRequestExecutor<>() {
            @Override
            public void executeRemoteRequest(
                TransportService transportService,
                Transport.Connection conn,
                FieldCapabilitiesRequest remoteRequest,
                ActionListenerResponseHandler<FieldCapabilitiesResponse> responseHandler
            ) {
                // Views are asked for, datasets are not: a remote dataset is invisible to this query rather than an error,
                // so a name that matches one there falls through to normal remote index resolution and resolves to nothing.
                remoteRequest.indicesOptions(
                    IndicesOptions.builder(remoteRequest.indicesOptions())
                        .indexAbstractionOptions(
                            IndicesOptions.IndexAbstractionOptions.builder(remoteRequest.indicesOptions().indexAbstractionOptions())
                                .resolveViews(true)
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

    private ElasticsearchException validateNoRemoteViews(EsqlResolveFieldsRequest request) {
        // resolveViews is only set on a request from the originating cluster, so this detection runs only on a remote
        // cluster. A view is not remotable and a query that reaches one across a cluster boundary fails rather than
        // silently reading less than it named.
        var abstractionOptions = request.indicesOptions().indexAbstractionOptions();
        List<String> remoteViews = abstractionOptions.resolveViews()
            ? qualify(
                request.fieldCapsRequest().clusterAlias(),
                getViews(request.indices(), request.indicesOptions(), request.getResolvedIndexExpressions())
            )
            : List.of();
        return remoteViews.isEmpty() ? null : new RemoteViewNotSupportedException(remoteViews);
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

}
