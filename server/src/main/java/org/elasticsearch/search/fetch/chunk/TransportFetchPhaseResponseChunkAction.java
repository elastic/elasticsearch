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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport action that receives fetch result chunks from data nodes. This action runs on the coordinator node and serves as
 * the receiver endpoint for {@link FetchPhaseResponseChunk} messages sent by data nodes during chunked fetch operations.
 */
public class TransportFetchPhaseResponseChunkAction extends HandledTransportAction<
    TransportFetchPhaseResponseChunkAction.Request,
    ActionResponse.Empty> {

    /*
     * [Data Node]                                   [Coordinator]
     *    |                                               |
     *    | FetchPhase.execute(writer)                    |
     *    |   ↓                                           |
     *    | writer.writeResponseChunk(chunk) ------------>| TransportFetchPhaseResponseChunkAction
     *    |                                               |     ↓
     *    |                                               | activeFetchPhaseTasks.acquireResponseStream()
     *    |                                               |     ↓
     *    |                                               | responseStream.writeChunk()
     *    |                                               |
     *    |<------------- [ACK (Empty)]------- -----------|
     *
     */

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("indices:data/read/fetch/chunk");

    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;

    /**
     * Creates a new chunk receiver action.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param activeFetchPhaseTasks the registry of active fetch response streams
     */
    @Inject
    public TransportFetchPhaseResponseChunkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ActiveFetchPhaseTasks activeFetchPhaseTasks
    ) {
        super(TYPE.name(), transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
    }

    /**
     * Request wrapper containing the coordinating task ID and the chunk contents.
     */
    public static class Request extends LegacyActionRequest {
        private long coordinatingTaskId;
        private FetchPhaseResponseChunk chunkContents;

        /**
         * Creates a new chunk request.
         *
         * @param coordinatingTaskId the ID of the coordinating search task
         * @param chunkContents the chunk to deliver
         */
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

    /**
     *  Running on the coordinator node. Processes an incoming chunk by routing it to the appropriate response stream.
     * <p>
     * This method:
     * <ol>
     *   <li>Extracts the shard ID from the chunk</li>
     *   <li>Acquires the response stream from {@link ActiveFetchPhaseTasks}</li>
     *   <li>Delegates to {@link FetchPhaseResponseStream#writeChunk}</li>
     *   <li>Releases the response stream reference</li>
     *   <li>Sends an acknowledgment response to the data node</li>
     * </ol>
     *
     * @param task the current task
     * @param request the chunk request
     * @param listener callback for sending the acknowledgment
     */
    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        ActionListener.run(listener, l -> {
            int shardId = request.chunkContents().shardIndex();
            long coordTaskId = request.coordinatingTaskId;

            final var responseStream = activeFetchPhaseTasks.acquireResponseStream(coordTaskId, shardId);
            try {
                if (request.chunkContents.type() == FetchPhaseResponseChunk.Type.HITS) {
                    responseStream.writeChunk(request.chunkContents(), () -> l.onResponse(ActionResponse.Empty.INSTANCE));
                }
            } finally {
                responseStream.decRef();
            }
        });
    }
}
