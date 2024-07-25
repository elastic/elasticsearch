/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction.Request;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class VerifyNodeRepositoryCoordinationAction {

    public static final String NAME = "internal:admin/repository/verify/coordinate";
    public static final ActionType<VerifyNodeRepositoryCoordinationAction.Response> TYPE = new ActionType<>(NAME);

    private VerifyNodeRepositoryCoordinationAction() {}

    public static class Response extends ActionResponse {

        final List<DiscoveryNode> nodes;

        public Response(List<DiscoveryNode> nodes) {
            this.nodes = nodes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class LocalAction extends TransportAction<Request, Response> {

        private final TransportService transportService;
        private final ClusterService clusterService;
        private final NodeClient client;

        @Inject
        public LocalAction(
            ActionFilters actionFilters,
            TransportService transportService,
            ClusterService clusterService,
            NodeClient client
        ) {
            super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.transportService = transportService;
            this.clusterService = clusterService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final DiscoveryNodes discoNodes = clusterService.state().nodes();
            final DiscoveryNode localNode = discoNodes.getLocalNode();

            final Collection<DiscoveryNode> masterAndDataNodes = discoNodes.getMasterAndDataNodes().values();
            final List<DiscoveryNode> nodes = new ArrayList<>();
            for (DiscoveryNode node : masterAndDataNodes) {
                if (RepositoriesService.isDedicatedVotingOnlyNode(node.getRoles()) == false) {
                    nodes.add(node);
                }
            }
            final CopyOnWriteArrayList<VerificationFailure> errors = new CopyOnWriteArrayList<>();
            final AtomicInteger counter = new AtomicInteger(nodes.size());
            for (final DiscoveryNode node : nodes) {
                transportService.sendRequest(
                    node,
                    VerifyNodeRepositoryAction.ACTION_NAME,
                    request,
                    new TransportResponseHandler<ActionResponse.Empty>() {

                        @Override
                        public ActionResponse.Empty read(StreamInput in) throws IOException {
                            return ActionResponse.Empty.INSTANCE;
                        }

                        @Override
                        public Executor executor() {
                            return TransportResponseHandler.TRANSPORT_WORKER;
                        }

                        @Override
                        public void handleResponse(ActionResponse.Empty _ignore) {
                            if (counter.decrementAndGet() == 0) {
                                finishVerification(request.repository, listener, nodes, errors);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            errors.add(new VerificationFailure(node.getId(), exp));
                            if (counter.decrementAndGet() == 0) {
                                finishVerification(request.repository, listener, nodes, errors);
                            }
                        }
                    }
                );
            }
        }

        private static void finishVerification(
            String repositoryName,
            ActionListener<Response> listener,
            List<DiscoveryNode> nodes,
            CopyOnWriteArrayList<VerificationFailure> errors
        ) {
            if (errors.isEmpty() == false) {
                RepositoryVerificationException e = new RepositoryVerificationException(repositoryName, errors.toString());
                for (VerificationFailure error : errors) {
                    e.addSuppressed(error.getCause());
                }
                listener.onFailure(e);
            } else {
                listener.onResponse(new Response(nodes));
            }
        }
    }
}
