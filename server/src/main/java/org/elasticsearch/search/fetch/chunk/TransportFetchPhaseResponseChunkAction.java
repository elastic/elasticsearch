/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;// package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * This is the receiver for chunk requests from the data node.
 * Receives chunk transport requests from the data node and forwards them into the response stream.
 */
public class TransportFetchPhaseResponseChunkAction extends HandledTransportAction<
    TransportFetchPhaseResponseChunkAction.Request,
    ActionResponse.Empty> {

    public static final String ACTION_NAME = "internal:search/fetch/chunk";

    //static final String ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[response_chunk]";

    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;

    TransportFetchPhaseResponseChunkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Executor executor,
        ActiveFetchPhaseTasks activeFetchPhaseTasks
    ) {
        super(ACTION_NAME, transportService, actionFilters, Request::new, executor);
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
    }

    public static class Request extends LegacyActionRequest {
        private long coordinatingTaskId;
        private FetchPhaseResponseChunk chunkContents;

        public Request(long coordinatingTaskId, FetchPhaseResponseChunk chunkContents) {
            this.coordinatingTaskId = coordinatingTaskId;
            this.chunkContents = Objects.requireNonNull(chunkContents);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingTaskId = in.readVLong();
            chunkContents = new FetchPhaseResponseChunk(in);
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

        public FetchPhaseResponseChunk chunkContents() {
            return chunkContents;
        }
    }

    // Running on the coordinator node, receives chunk requests from the data node (FetchPhaseResponseChunk)
    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        ActionListener.run(listener, l -> {
            final var responseStream = activeFetchPhaseTasks.acquireResponseStream(request.coordinatingTaskId);
            try {
                if (request.chunkContents.type() == FetchPhaseResponseChunk.Type.START_RESPONSE) {
                    responseStream.startResponse(() -> l.onResponse(ActionResponse.Empty.INSTANCE));
                } else if (request.chunkContents.type() == FetchPhaseResponseChunk.Type.HITS) {
                    responseStream.writeChunk(request.chunkContents(), () -> l.onResponse(ActionResponse.Empty.INSTANCE));
                }
            } finally {
                responseStream.decRef();
            }
        });
    }
}
