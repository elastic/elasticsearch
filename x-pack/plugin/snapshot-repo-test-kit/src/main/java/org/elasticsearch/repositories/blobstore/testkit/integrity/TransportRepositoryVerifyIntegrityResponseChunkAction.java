/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityResponseChunkAction extends HandledTransportAction<
    TransportRepositoryVerifyIntegrityResponseChunkAction.Request,
    ActionResponse.Empty> {

    static final String ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[response_chunk]";

    private final ActiveRepositoryVerifyIntegrityTasks activeRepositoryVerifyIntegrityTasks;

    public TransportRepositoryVerifyIntegrityResponseChunkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Executor executor,
        ActiveRepositoryVerifyIntegrityTasks activeRepositoryVerifyIntegrityTasks
    ) {
        super(ACTION_NAME, transportService, actionFilters, Request::new, executor);
        this.activeRepositoryVerifyIntegrityTasks = activeRepositoryVerifyIntegrityTasks;
    }

    public static class Request extends ActionRequest {
        private final long coordinatingTaskId;
        private final RepositoryVerifyIntegrityResponseChunk chunkContents;

        public Request(long coordinatingTaskId, RepositoryVerifyIntegrityResponseChunk chunkContents) {
            this.coordinatingTaskId = coordinatingTaskId;
            this.chunkContents = Objects.requireNonNull(chunkContents);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            coordinatingTaskId = in.readVLong();
            chunkContents = new RepositoryVerifyIntegrityResponseChunk(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(coordinatingTaskId);
            chunkContents.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public RepositoryVerifyIntegrityResponseChunk chunkContents() {
            return chunkContents;
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        ActionListener.run(listener, l -> {
            final var ongoingRequest = activeRepositoryVerifyIntegrityTasks.acquire(request.coordinatingTaskId);
            try {
                if (request.chunkContents().type() == RepositoryVerifyIntegrityResponseChunk.Type.START_RESPONSE) {
                    ongoingRequest.startResponse(() -> l.onResponse(ActionResponse.Empty.INSTANCE));
                } else {
                    ongoingRequest.writeChunk(request.chunkContents(), () -> l.onResponse(ActionResponse.Empty.INSTANCE));
                }
            } finally {
                ongoingRequest.decRef();
            }
        });
    }
}
