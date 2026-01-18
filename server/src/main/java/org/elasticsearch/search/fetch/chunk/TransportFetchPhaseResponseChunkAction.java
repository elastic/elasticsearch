/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport action that receives fetch result chunks from data nodes. This action runs on the
 * coordinator node and serves as the receiver endpoint for {@link FetchPhaseResponseChunk}
 * messages sent by data nodes during chunked fetch operations.
 *
 * <p>Supports two transport modes:
 * <ul>
 *   <li><b>Zero-copy mode ({@link #ZERO_COPY_ACTION_NAME})</b>: Chunks arrive as {@link BytesTransportRequest}.
 *          Bytes flow directly from Netty buffers without copying.</li>
 *   <li><b>Standard mode ({@link #TYPE})</b>: Chunks arrive as {@link Request} objects via
 *       standard HandledTransportAction path.</li>
 * </ul>
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
     *    |   (via BytesTransportRequest, zero-copy)      |     ↓
     *    |                                               | activeFetchPhaseTasks.acquireResponseStream()
     *    |                                               |     ↓
     *    |                                               | responseStream.writeChunk()
     *    |                                               |
     *    |<------------- [ACK (Empty)]-------------------|
     */

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("internal:data/read/search/fetch/chunk");

    /**
     * Action name for zero-copy BytesTransportRequest path.
     * Sender uses this action name when sending via BytesTransportRequest.
     */
    public static final String ZERO_COPY_ACTION_NAME = TYPE.name() + "[bytes]";

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
        registerZeroCopyHandler(transportService);
    }

    /**
     * Registers the handler for zero-copy chunk reception via BytesTransportRequest.
     * The incoming bytes contain a routing header (coordinatingTaskId) followed by the chunk data.
     * We parse the header to extract the task ID, then deserialize and process the chunk.
     */
    private void registerZeroCopyHandler(TransportService transportService) {
        transportService.registerRequestHandler(
            ZERO_COPY_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            true,
            BytesTransportRequest::new,
            (request, channel, task) -> {
                ReleasableBytesReference bytesRef = request.bytes();
                FetchPhaseResponseChunk chunk = null;
                boolean handedOff = false;

                try (StreamInput in = bytesRef.streamInput()) {
                    long coordinatingTaskId = in.readVLong();
                    chunk = new FetchPhaseResponseChunk(in);

                    processChunk(
                        coordinatingTaskId,
                        chunk,
                        ActionListener.running(() -> { channel.sendResponse(ActionResponse.Empty.INSTANCE); })
                    );
                    handedOff = true;
                } catch (Exception e) {
                    channel.sendResponse(e);
                    if (handedOff == false && chunk != null) {
                        chunk.close();
                    } else if (handedOff == false) {
                        bytesRef.close();
                    }
                }
            }
        );
    }

    /**
     * Request wrapper containing the coordinating task ID and the chunk contents.
     */
    public static class Request extends LegacyActionRequest implements IndicesRequest {
        private long coordinatingTaskId;
        private FetchPhaseResponseChunk chunkContents;

        private String[] indices;
        private IndicesOptions indicesOptions;

        /**
         * Creates a new chunk request.
         *
         * @param coordinatingTaskId the ID of the coordinating search task
         * @param chunkContents the chunk to deliver
         * @param indices the indices being searched
         * @param indicesOptions the indices options
         */
        public Request(long coordinatingTaskId, FetchPhaseResponseChunk chunkContents, String[] indices, IndicesOptions indicesOptions) {
            this.coordinatingTaskId = coordinatingTaskId;
            this.chunkContents = Objects.requireNonNull(chunkContents);
            this.indices = indices;
            this.indicesOptions = indicesOptions;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingTaskId = in.readVLong();
            chunkContents = new FetchPhaseResponseChunk(in);
            this.indices = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(coordinatingTaskId);
            chunkContents.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public FetchPhaseResponseChunk chunkContents() {
            return chunkContents;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }
    }

    /**
     * Processes Request directly via HandledTransportAction.
     *
     * @param task the current task
     * @param request the chunk request
     * @param listener callback for sending the acknowledgment
     */
    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        processChunk(request.coordinatingTaskId, request.chunkContents(), listener);
    }

    /**
     *  Running on the coordinator node. Processes an incoming chunk by routing it to the appropriate response stream.
     *
     * <p>This method:
     * <ol>
     *   <li>Extracts the shard ID from the chunk</li>
     *   <li>Acquires the response stream from {@link ActiveFetchPhaseTasks}</li>
     *   <li>Delegates to {@link FetchPhaseResponseStream#writeChunk}</li>
     *   <li>Releases the response stream reference</li>
     *   <li>Sends an acknowledgment response to the data node</li>
     * </ol>
     *
     * @param coordinatingTaskId the ID of the coordinating search task
     * @param chunk the chunk to process
     * @param listener callback for sending the acknowledgment
     */
    private void processChunk(long coordinatingTaskId, FetchPhaseResponseChunk chunk, ActionListener<ActionResponse.Empty> listener) {
        ActionListener.run(listener, l -> {
            ShardId shardId = chunk.shardId();

            final var responseStream = activeFetchPhaseTasks.acquireResponseStream(coordinatingTaskId, shardId);
            try {
                if (chunk.type() == FetchPhaseResponseChunk.Type.HITS) {
                    responseStream.writeChunk(chunk, () -> l.onResponse(ActionResponse.Empty.INSTANCE));
                }
            } finally {
                responseStream.decRef();
            }
        });
    }
}
