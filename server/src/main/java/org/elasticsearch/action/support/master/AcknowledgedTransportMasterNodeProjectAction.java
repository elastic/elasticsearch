/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public abstract class AcknowledgedTransportMasterNodeProjectAction<Request extends MasterNodeRequest<Request>> extends
    AcknowledgedTransportMasterNodeAction<Request> {

    private final ProjectResolver projectResolver;

    public AcknowledgedTransportMasterNodeProjectAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        ProjectResolver projectResolver,
        Executor executor
    ) {
        super(actionName, canTripCircuitBreaker, transportService, clusterService, threadPool, actionFilters, request, executor);
        this.projectResolver = projectResolver;
    }

    public AcknowledgedTransportMasterNodeProjectAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        ProjectResolver projectResolver,
        Executor executor
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, executor);
        this.projectResolver = projectResolver;
    }

    protected abstract void masterOperation(
        Task task,
        Request request,
        ProjectState projectState,
        ActionListener<AcknowledgedResponse> listener
    );

    @Override
    protected final void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        masterOperation(task, request, projectResolver.getProjectState(state), listener);
    }

    protected abstract ClusterBlockException checkBlock(Request request, ProjectState projectState);

    @Override
    protected final ClusterBlockException checkBlock(Request request, ClusterState state) {
        return checkBlock(request, projectResolver.getProjectState(state));
    }

}
