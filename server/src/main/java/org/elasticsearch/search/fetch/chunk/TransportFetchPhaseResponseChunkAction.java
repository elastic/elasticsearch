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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportService;

/**
 * Receives fetch result chunks from data nodes via zero-copy transport. This component runs on the
 * coordinator node and serves as the receiver endpoint for {@link FetchPhaseResponseChunk}
 * messages sent by data nodes during chunked fetch operations.
 *
 * <p>Chunks arrive as {@link BytesTransportRequest} on the {@link #ZERO_COPY_ACTION_NAME} endpoint.
 * Bytes flow directly from Netty buffers without an intermediate deserialization/re-serialization step.
 */
public class TransportFetchPhaseResponseChunkAction {

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

    /**
     * Action name for zero-copy BytesTransportRequest path.
     * Sender uses this action name when sending via BytesTransportRequest.
     */
    public static final String ZERO_COPY_ACTION_NAME = "internal:data/read/search/fetch/chunk[bytes]";

    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;

    /**
     * Required for deserializing SearchHits that contain NamedWriteable objects.
     * <p>
     * SearchHit's DocumentFields can contain types like {@link org.elasticsearch.search.fetch.subphase.LookupField}
     * which implement NamedWriteable. When reading serialized hits from raw bytes (from chunks),
     * the basic StreamInput cannot deserialize these types. Wrapping with
     * {@link NamedWriteableAwareStreamInput} provides the registry needed to resolve
     * NamedWriteable types by their registered names.
     */
    private final NamedWriteableRegistry namedWriteableRegistry;

    /**
     * Creates a new chunk receiver and registers the zero-copy transport handler.
     *
     * @param transportService the transport service used to register the handler
     * @param activeFetchPhaseTasks the registry of active fetch response streams
     * @param namedWriteableRegistry registry for deserializing NamedWriteable types in chunks
     */
    @Inject
    public TransportFetchPhaseResponseChunkAction(
        TransportService transportService,
        ActiveFetchPhaseTasks activeFetchPhaseTasks,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
        this.namedWriteableRegistry = namedWriteableRegistry;
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
                long coordinatingTaskId;
                FetchPhaseResponseChunk chunk;

                try (StreamInput in = new NamedWriteableAwareStreamInput(bytesRef.streamInput(), namedWriteableRegistry)) {
                    coordinatingTaskId = in.readVLong();
                    chunk = new FetchPhaseResponseChunk(in);
                } catch (Exception e) {
                    channel.sendResponse(e);
                    return;
                }

                processChunk(
                    coordinatingTaskId,
                    chunk,
                    ActionListener.releaseAfter(
                        ActionListener.wrap(ignored -> channel.sendResponse(ActionResponse.Empty.INSTANCE), channel::sendResponse),
                        chunk
                    )
                );
            }
        );
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
                responseStream.writeChunk(chunk, () -> l.onResponse(ActionResponse.Empty.INSTANCE));
            } finally {
                responseStream.decRef();
            }
        });
    }
}
