/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class PutInternalCcrRepositoryAction extends Action<PutInternalCcrRepositoryAction.PutInternalCcrRepositoryResponse> {

    public static final PutInternalCcrRepositoryAction INSTANCE = new PutInternalCcrRepositoryAction();
    public static final String NAME = "internal:admin/ccr/internal_repository/put";

    private PutInternalCcrRepositoryAction() {
        super(NAME);
    }

    @Override
    public PutInternalCcrRepositoryResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<PutInternalCcrRepositoryResponse> getResponseReader() {
        return PutInternalCcrRepositoryResponse::new;
    }

    public static class TransportPutInternalRepositoryAction
        extends TransportAction<PutInternalCcrRepositoryRequest, PutInternalCcrRepositoryResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportPutInternalRepositoryAction(RepositoriesService repositoriesService, ActionFilters actionFilters,
                                                    TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, PutInternalCcrRepositoryRequest request,
                                 ActionListener<PutInternalCcrRepositoryResponse> listener) {
            repositoriesService.registerInternalRepository(request.getName(), request.getType());
            listener.onResponse(new PutInternalCcrRepositoryResponse());
        }
    }

    public static class PutInternalCcrRepositoryResponse extends ActionResponse {

        PutInternalCcrRepositoryResponse() {
            super();
        }

        PutInternalCcrRepositoryResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
        }
    }
}
