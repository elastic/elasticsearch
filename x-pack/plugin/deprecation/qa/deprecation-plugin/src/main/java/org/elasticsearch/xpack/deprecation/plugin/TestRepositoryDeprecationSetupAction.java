/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.EmptyResponseListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class TestRepositoryDeprecationSetupAction {

    public static final String NAME = "cluster:admin/xpack/deprecation/test/repository_setup";
    public static final String INVALID_REPOSITORY_TYPE = "invalid_for_deprecation_test";
    public static final String UNKNOWN_REPOSITORY_TYPE = "unknown_for_deprecation_test";
    public static final String INVALID_REPOSITORY_NAME = "invalid-repository";
    public static final String UNKNOWN_REPOSITORY_NAME = "unknown-repository";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);

    private TestRepositoryDeprecationSetupAction() {}

    public static Map.Entry<String, Repository.Factory> invalidRepositoryEntry() {
        return Map.entry(INVALID_REPOSITORY_TYPE, (projectId, metadata) -> {
            throw new RepositoryException(metadata.name(), "repository construction failed");
        });
    }

    public static class Request extends MasterNodeRequest<Request> {
        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public String getName() {
            return "test_repository_deprecation_setup_action";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/_test_cluster/deprecation/create_test_repositories"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            final var setupRequest = new Request(getMasterNodeTimeout(request));
            return channel -> client.execute(TYPE, setupRequest, new EmptyResponseListener(channel));
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, ActionResponse.Empty> {

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(
                NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                in -> ActionResponse.Empty.INSTANCE,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<ActionResponse.Empty> listener) {
            submitUnbatchedTask(new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final ProjectMetadata project = currentState.metadata().getProject(ProjectId.DEFAULT);
                    final RepositoriesMetadata existingRepositories = RepositoriesMetadata.get(project);
                    if (existingRepositories.repository(INVALID_REPOSITORY_NAME) != null) {
                        throw new IllegalArgumentException("repository [" + INVALID_REPOSITORY_NAME + "] already exists");
                    }
                    if (existingRepositories.repository(UNKNOWN_REPOSITORY_NAME) != null) {
                        throw new IllegalArgumentException("repository [" + UNKNOWN_REPOSITORY_NAME + "] already exists");
                    }

                    final List<RepositoryMetadata> repositories = new ArrayList<>(existingRepositories.repositories());
                    repositories.add(new RepositoryMetadata(INVALID_REPOSITORY_NAME, INVALID_REPOSITORY_TYPE, Settings.EMPTY));
                    repositories.add(new RepositoryMetadata(UNKNOWN_REPOSITORY_NAME, UNKNOWN_REPOSITORY_TYPE, Settings.EMPTY));

                    final ProjectMetadata updatedProject = ProjectMetadata.builder(project)
                        .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositories))
                        .build();
                    return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    listener.onResponse(ActionResponse.Empty.INSTANCE);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        @SuppressForbidden(reason = "legacy usage of unbatched task")
        private void submitUnbatchedTask(ClusterStateUpdateTask task) {
            clusterService.submitUnbatchedStateUpdateTask("inject test repository metadata", task);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
