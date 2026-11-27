/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.ql.plugin.AbstractTransportQlAsyncGetResultsAction;

public class TransportEqlAsyncGetResultsAction extends AbstractTransportQlAsyncGetResultsAction<EqlSearchResponse, EqlSearchTask> {

    /** Breaker label for a relayed get async EQL response, whose events carry their source like the hits of a search. */
    public static final String RELAYED_RESPONSE_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_FETCH + "[async_eql_response]";

    @Inject
    public TransportEqlAsyncGetResultsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService
    ) {
        super(
            EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME,
            transportService,
            actionFilters,
            clusterService,
            registry,
            client,
            threadPool,
            bigArrays,
            circuitBreakerService,
            EqlSearchTask.class
        );
    }

    @Override
    public Writeable.Reader<EqlSearchResponse> responseReader() {
        return EqlSearchResponse::new;
    }

    @Override
    protected String relayedResponseBreakerLabel() {
        return RELAYED_RESPONSE_BREAKER_LABEL;
    }
}
