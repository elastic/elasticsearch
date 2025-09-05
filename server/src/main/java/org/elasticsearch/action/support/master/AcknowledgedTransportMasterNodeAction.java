/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * Base class for the common case of a {@link TransportMasterNodeAction} that responds with an {@link AcknowledgedResponse}.
 */
public abstract class AcknowledgedTransportMasterNodeAction<Request extends MasterNodeRequest<Request>> extends TransportMasterNodeAction<
    Request,
    AcknowledgedResponse> {

    protected AcknowledgedTransportMasterNodeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Executor executor
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, AcknowledgedResponse::readFrom, executor);
    }

    protected AcknowledgedTransportMasterNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Executor executor
    ) {
        super(
            actionName,
            canTripCircuitBreaker,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            AcknowledgedResponse::readFrom,
            executor
        );
    }
}
