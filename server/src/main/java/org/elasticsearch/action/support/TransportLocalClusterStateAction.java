/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * Analogue of {@link org.elasticsearch.action.support.master.TransportMasterNodeReadAction} except that it runs on the local node rather
 * than delegating to the master.
 */
public abstract class TransportLocalClusterStateAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final Executor executor;

    protected TransportLocalClusterStateAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Executor executor
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(actionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.executor = executor;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var state = clusterService.state();
        final var clusterBlockException = checkBlock(request, state);
        if (clusterBlockException != null) {
            throw clusterBlockException;
        }

        // Workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        executor.execute(ActionRunnable.wrap(listener, l -> localClusterStateOperation(task, request, state, l)));
    }

    protected abstract void localClusterStateOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;
}
