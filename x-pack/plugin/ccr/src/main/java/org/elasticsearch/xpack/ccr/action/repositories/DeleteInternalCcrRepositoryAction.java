/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class DeleteInternalCcrRepositoryAction extends ActionType<ActionResponse.Empty> {

    public static final DeleteInternalCcrRepositoryAction INSTANCE = new DeleteInternalCcrRepositoryAction();
    public static final String NAME = "internal:admin/ccr/internal_repository/delete";

    private DeleteInternalCcrRepositoryAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class TransportDeleteInternalRepositoryAction extends TransportAction<
        DeleteInternalCcrRepositoryRequest,
        ActionResponse.Empty> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportDeleteInternalRepositoryAction(
            RepositoriesService repositoriesService,
            ActionFilters actionFilters,
            TransportService transportService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, DeleteInternalCcrRepositoryRequest request, ActionListener<ActionResponse.Empty> listener) {
            repositoriesService.unregisterInternalRepository(request.getName());
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }
    }
}
