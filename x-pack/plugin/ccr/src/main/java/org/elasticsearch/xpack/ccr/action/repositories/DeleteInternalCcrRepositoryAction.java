/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class DeleteInternalCcrRepositoryAction extends ActionType<DeleteInternalCcrRepositoryAction.DeleteInternalCcrRepositoryResponse> {

    public static final DeleteInternalCcrRepositoryAction INSTANCE = new DeleteInternalCcrRepositoryAction();
    public static final String NAME = "internal:admin/ccr/internal_repository/delete";

    private DeleteInternalCcrRepositoryAction() {
        super(NAME, DeleteInternalCcrRepositoryAction.DeleteInternalCcrRepositoryResponse::new);
    }

    public static class TransportDeleteInternalRepositoryAction
        extends TransportAction<DeleteInternalCcrRepositoryRequest, DeleteInternalCcrRepositoryResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportDeleteInternalRepositoryAction(RepositoriesService repositoriesService, ActionFilters actionFilters,
                                                       TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, DeleteInternalCcrRepositoryRequest request,
                                 ActionListener<DeleteInternalCcrRepositoryResponse> listener) {
            repositoriesService.unregisterInternalRepository(request.getName());
            listener.onResponse(new DeleteInternalCcrRepositoryResponse());
        }
    }

    public static class DeleteInternalCcrRepositoryResponse extends ActionResponse {

        DeleteInternalCcrRepositoryResponse() {
            super();
        }

        DeleteInternalCcrRepositoryResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
