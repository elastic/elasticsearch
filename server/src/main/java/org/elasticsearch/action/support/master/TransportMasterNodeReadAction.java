/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * A base class for read operations that needs to be performed on the master node.
 * Can also be executed on the local node if needed.
 */
public abstract class TransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse> extends
    TransportMasterNodeAction<Request, Response> {

    protected TransportMasterNodeReadAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<Response> response,
        Executor executor
    ) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, response, executor);
    }

    protected TransportMasterNodeReadAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<Response> response,
        Executor executor
    ) {
        super(actionName, canTripCircuitBreaker, transportService, clusterService, threadPool, actionFilters, request, response, executor);
    }

    @Override
    protected final boolean localExecute(Request request) {
        return request.local();
    }
}
