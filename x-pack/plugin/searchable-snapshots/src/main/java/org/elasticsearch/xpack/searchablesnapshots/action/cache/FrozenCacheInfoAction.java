/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action.cache;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class FrozenCacheInfoAction extends ActionType<FrozenCacheInfoResponse> {

    public static final String NAME = "internal:admin/xpack/searchable_snapshots/frozen_cache_info";
    public static final FrozenCacheInfoAction INSTANCE = new FrozenCacheInfoAction();

    private FrozenCacheInfoAction() {
        super(NAME, FrozenCacheInfoResponse::new);
    }

    public static class Request extends ActionRequest {

        private final DiscoveryNode discoveryNode;

        public Request(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            discoveryNode = new DiscoveryNode(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return discoveryNode.toString();
        }
    }

    public static class TransportAction extends HandledTransportAction<FrozenCacheInfoAction.Request, FrozenCacheInfoResponse> {

        private final FrozenCacheInfoNodeAction.Request nodeRequest = new FrozenCacheInfoNodeAction.Request();
        private final TransportService transportService;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            super(NAME, transportService, actionFilters, FrozenCacheInfoAction.Request::new);
            this.transportService = transportService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<FrozenCacheInfoResponse> listener) {
            if (request.discoveryNode.getVersion().onOrAfter(Version.V_7_12_0)) {
                transportService.sendChildRequest(
                    request.discoveryNode,
                    FrozenCacheInfoNodeAction.NAME,
                    nodeRequest,
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(listener, FrozenCacheInfoResponse::new)
                );
            } else {
                listener.onResponse(new FrozenCacheInfoResponse(false));
            }
        }

    }

}
