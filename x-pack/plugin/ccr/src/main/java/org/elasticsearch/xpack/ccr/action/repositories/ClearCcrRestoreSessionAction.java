/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;
import java.util.List;

public class ClearCcrRestoreSessionAction extends Action<ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse> {

    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/clear";

    private ClearCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public ClearCcrRestoreSessionResponse newResponse() {
        return new ClearCcrRestoreSessionResponse();
    }

    public static class TransportDeleteCcrRestoreSessionAction extends TransportNodesAction<ClearCcrRestoreSessionRequest,
        ClearCcrRestoreSessionResponse, ClearCcrRestoreSessionRequest.Request, Response> {

        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(ThreadPool threadPool, ClusterService clusterService, ActionFilters actionFilters,
                                                      TransportService transportService, CcrRestoreSourceService ccrRestoreService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, ClearCcrRestoreSessionRequest::new,
                ClearCcrRestoreSessionRequest.Request::new, ThreadPool.Names.GENERIC, Response.class);
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected ClearCcrRestoreSessionResponse newResponse(ClearCcrRestoreSessionRequest request, List<Response> responses,
                                                             List<FailedNodeException> failures) {
            return new ClearCcrRestoreSessionResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected ClearCcrRestoreSessionRequest.Request newNodeRequest(String nodeId, ClearCcrRestoreSessionRequest request) {
            return request.getRequest();
        }

        @Override
        protected Response newNodeResponse() {
            return new Response();
        }

        @Override
        protected Response nodeOperation(ClearCcrRestoreSessionRequest.Request request) {
            ccrRestoreService.closeSession(request.getSessionUUID());
            return new Response(clusterService.localNode());
        }
    }

    public static class Response extends BaseNodeResponse {

        private Response() {
        }

        private Response(StreamInput in) throws IOException {
            readFrom(in);
        }

        private Response(DiscoveryNode node) {
            super(node);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }
    }

    public static class ClearCcrRestoreSessionResponse extends BaseNodesResponse<Response> {

        ClearCcrRestoreSessionResponse() {
        }

        ClearCcrRestoreSessionResponse(ClusterName clusterName, List<Response> chunkResponses, List<FailedNodeException> failures) {
            super(clusterName, chunkResponses, failures);
        }

        @Override
        protected List<Response> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(Response::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<Response> nodes) throws IOException {
            out.writeList(nodes);
        }
    }
}
