/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.TransportRelayAction;
import org.elasticsearch.xpack.core.security.action.TransportRelayRequest;
import org.elasticsearch.xpack.core.security.action.TransportRelayResponse;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.function.Function;

/**
 * Clears a security cache by name (with optional keys).
 * @see CacheInvalidatorRegistry
 */
public class TransportTransportRelayAction extends HandledTransportAction<TransportRelayRequest, TransportRelayResponse> {

    private final NodeClient nodeClient;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportTransportRelayAction(
        NodeClient nodeClient,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(TransportRelayAction.NAME, transportService, actionFilters, TransportRelayRequest::new);
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, TransportRelayRequest request, ActionListener<TransportRelayResponse> listener) {
        final RequestHandlerRegistry<? extends TransportRequest> requestHandler = transportService.getRequestHandler(request.getAction());

        final TransportRequest transportRequest;
        try {
            final byte[] decodedPayload = Base64.getDecoder().decode(request.getPayload());
            transportRequest = requestHandler.newRequest(
                new NamedWriteableAwareStreamInput(new ByteArrayStreamInput(decodedPayload), nodeClient.getNamedWriteableRegistry())
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final ActionListenerResponseHandler<TransportResponse> actionListenerResponseHandler = new ActionListenerResponseHandler<>(
            listener.map((actionResponse -> {
                final BytesStreamOutput out = new BytesStreamOutput();
                try {
                    actionResponse.writeTo(out);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new TransportRelayResponse(Base64.getEncoder().encodeToString(out.bytes().array()));
            })),
            getResponseReader(requestHandler, request.getAction(), transportRequest)
        );

        transportService.sendChildRequest(
            clusterService.localNode(),
            request.getAction(),
            transportRequest,
            task,
            TransportRequestOptions.EMPTY,
            actionListenerResponseHandler
        );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T extends TransportResponse> Writeable.Reader<T> getResponseReader(
        RequestHandlerRegistry<? extends TransportRequest> requestHandler,
        String action,
        TransportRequest request
    ) {
        final TransportRequestHandler<?> unwrappedHandler = ((SecurityServerTransportInterceptor.ProfileSecuredRequestHandler<
            ?>) requestHandler.getHandler()).getHandler();
        if (unwrappedHandler instanceof TransportActionProxy.ProxyRequestHandler proxyRequestHandler) {
            final Function<TransportRequest, Writeable.Reader<? extends TransportResponse>> responseFunction = proxyRequestHandler
                .getResponseFunction();
            return in -> (T) responseFunction.apply(request).read(in);
        }

        if (request instanceof ShardSearchRequest shardSearchRequest) {
            assert SearchTransportService.QUERY_ACTION_NAME.equals(action);
            final boolean fetchDocuments = shardSearchRequest.numberOfShards() == 1;
            return fetchDocuments ? in -> (T) new QueryFetchSearchResult(in) : in -> (T) new QuerySearchResult(in, true);
        } else if (request instanceof ShardFetchRequest shardFetchRequest) {
            assert SearchTransportService.FETCH_ID_ACTION_NAME.equals(action);
            return in -> (T) new FetchSearchResult(in);
        } else if (TransportOpenPointInTimeAction.OPEN_SHARD_READER_CONTEXT_NAME.equals(action)) {
            return in -> (T) new TransportOpenPointInTimeAction.ShardOpenReaderResponse(in);
        }
        return (Writeable.Reader<T>) nodeClient.getResponseReader(action);
    }
}
