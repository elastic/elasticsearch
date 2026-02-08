/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

public class TransportGetViewAction extends TransportLocalProjectMetadataAction<GetViewAction.Request, GetViewAction.Response> {
    public static final ActionType<RemoteInfoResponse> TYPE = new ActionType<>(GetViewAction.NAME);
    private final ViewService viewService;

    @Inject
    public TransportGetViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ViewService viewService
    ) {
        super(
            GetViewAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        this.viewService = viewService;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetViewAction.Request request,
        ProjectState project,
        ActionListener<GetViewAction.Response> listener
    ) {
        ProjectId projectId = project.projectId();
        Collection<View> views = new LinkedHashSet<>();
        List<String> missing = new ArrayList<>();
        String[] names = request.indices();
        // TODO currently doesn't support wildcards when security is off
        if (names == null || names.length == 0 || (names.length == 1 && Regex.isMatchAllPattern(names[0]))) {
            views = viewService.getMetadata(projectId).views().values();
        } else if (Arrays.equals(names, IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY) == false) {
            for (String name : names) {
                View view = viewService.get(projectId, name);
                if (view == null) {
                    // TODO currently doesn't throw an error when a concrete existing index is used as a view name in the API, returns empty
                    if (project.metadata().getIndicesLookup().containsKey(name) == false) {
                        missing.add(name);
                    }
                } else {
                    views.add(view);
                }
            }
        }
        if (missing.isEmpty() == false) {
            listener.onFailure(new ResourceNotFoundException("Views do not exist: " + String.join(", ", missing)));
        } else {
            listener.onResponse(new GetViewAction.Response(views));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetViewAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_READ);
    }
}
