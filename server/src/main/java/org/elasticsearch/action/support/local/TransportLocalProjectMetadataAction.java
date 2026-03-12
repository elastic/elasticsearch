/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.local;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.Executor;

/**
 * Analogue of {@link org.elasticsearch.action.support.master.TransportMasterNodeReadProjectAction} except that it runs on the local node
 * rather than delegating to the master.
 */
public abstract class TransportLocalProjectMetadataAction<Request extends LocalClusterStateRequest, Response extends ActionResponse> extends
    TransportLocalClusterStateAction<Request, Response> {

    private final ProjectResolver projectResolver;

    protected TransportLocalProjectMetadataAction(
        String actionName,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ClusterService clusterService,
        Executor executor,
        ProjectResolver projectResolver
    ) {
        super(actionName, actionFilters, taskManager, clusterService, executor);
        this.projectResolver = projectResolver;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ProjectState state);

    @Override
    protected final ClusterBlockException checkBlock(Request request, ClusterState state) {
        return checkBlock(request, projectResolver.getProjectState(state));
    }

    protected abstract void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener)
        throws Exception;

    @Override
    protected final void localClusterStateOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception {
        localClusterStateOperation(task, request, projectResolver.getProjectState(state), listener);
    }
}
