/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class VerifyNodeRepositoryAction {

    public static final String ACTION_NAME = "internal:admin/repository/verify";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(ACTION_NAME);

    // no construction
    private VerifyNodeRepositoryAction() {}

    public static class TransportAction extends HandledTransportAction<Request, ActionResponse.Empty> {

        private final ClusterService clusterService;
        private final RepositoriesService repositoriesService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ThreadPool threadPool,
            ClusterService clusterService,
            RepositoriesService repositoriesService
        ) {
            super(ACTION_NAME, transportService, actionFilters, Request::new, threadPool.executor(ThreadPool.Names.SNAPSHOT));
            this.clusterService = clusterService;
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
            DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
            try {
                Repository repository = repositoriesService.repository(request.repository);
                repository.verify(request.verificationToken, localNode);
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            } catch (Exception e) {
                logger.warn(() -> "[" + request.repository + "] failed to verify repository", e);
                listener.onFailure(e);
            }
        }
    }

    public static class Request extends ActionRequest {

        protected final String repository;
        protected final String verificationToken;

        public Request(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            verificationToken = in.readString();
        }

        Request(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

}
