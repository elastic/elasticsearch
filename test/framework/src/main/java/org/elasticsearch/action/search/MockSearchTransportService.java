/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class MockSearchTransportService extends SearchTransportService {
    public MockSearchTransportService(
        TransportService transportService,
        NodeClient client,
        BiFunction<
            Transport.Connection,
            SearchActionListener<? super SearchPhaseResult>,
            ActionListener<? super SearchPhaseResult>> responseWrapper
    ) {
        super(transportService, client, responseWrapper);
    }

    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockSearchTransportService}.
     */
    public static class TestPlugin extends Plugin {}

    final AtomicInteger freeContextRequests = new AtomicInteger(0);
    final AtomicInteger successfulFreeContextRequests = new AtomicInteger(0);
    final AtomicInteger failedFreeContextRequests = new AtomicInteger(0);

    public AtomicInteger getFreeContextRequests() {
        return freeContextRequests;
    }

    public AtomicInteger getSuccessfulFreeContextRequests() {
        return successfulFreeContextRequests;
    }

    public AtomicInteger getFailedFreeContextRequests() {
        return failedFreeContextRequests;
    }

    @Override
    public void sendFreeContext(Transport.Connection connection, final ShardSearchContextId contextId, OriginalIndices originalIndices) {
        freeContextRequests.incrementAndGet();
        transportService().sendRequest(
            connection,
            FREE_CONTEXT_ACTION_NAME,
            new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY,
            // no need to respond if it was freed or not
            new ActionListenerResponseHandler<>(new ActionListener<>() {
                @Override
                public void onResponse(SearchFreeContextResponse searchFreeContextResponse) {
                    if (searchFreeContextResponse.isFreed()) {
                        successfulFreeContextRequests.incrementAndGet();
                    } else {
                        failedFreeContextRequests.incrementAndGet();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failedFreeContextRequests.incrementAndGet();
                }
            }, SearchFreeContextResponse::new, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }
}
