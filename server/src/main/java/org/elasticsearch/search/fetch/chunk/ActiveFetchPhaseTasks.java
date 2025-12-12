/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;// package org.elasticsearch.action.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentMap;

/**
 * Manages the registry of active fetch response streams on the coordinator node.
 */

public final class ActiveFetchPhaseTasks {

    private final ConcurrentMap<ResponseStreamKey, FetchPhaseResponseStream> tasks = ConcurrentCollections.newConcurrentMap();

    /**
     * Registers a response stream for a specific coordinating task and shard.
     *
     * This method is called by {@link TransportFetchPhaseCoordinationAction} when starting
     * a chunked fetch. The returned {@link Releasable} must be closed when the fetch
     * completes to remove the stream from the registry.
     *
     * @param coordinatingTaskId the ID of the coordinating search task
     * @param shardId the shard ID being fetched
     * @param responseStream the stream to register (must have at least one reference count)
     * @return a releasable that removes the registration when closed
     * @throws IllegalStateException if a stream for this task+shard combination is already registered
     */
    Releasable registerResponseBuilder(long coordinatingTaskId, ShardId shardId, FetchPhaseResponseStream responseStream) {
        assert responseStream.hasReferences();

        ResponseStreamKey key = new ResponseStreamKey(coordinatingTaskId, shardId);

        final var previous = tasks.putIfAbsent(key, responseStream);
        if (previous != null) {
            final var exception = new IllegalStateException("already executing fetch task [" + coordinatingTaskId + "]");
            assert false : exception;
            throw exception;
        }

        return Releasables.assertOnce(() -> {
            final var removed = tasks.remove(key, responseStream);
            if (removed == false) {
                final var exception = new IllegalStateException("already completed fetch task [" + coordinatingTaskId + "]");
                assert false : exception;
                throw exception;
            }
        });
    }

    /**
     * Acquires the response stream for the given coordinating task and shard, incrementing its reference count.
     *
     * This method is called by {@link TransportFetchPhaseResponseChunkAction} for each arriving chunk.
     * The caller must call {@link FetchPhaseResponseStream#decRef()} when done processing the chunk.
     *
     * @param coordinatingTaskId the ID of the coordinating search task
     * @param shardId the shard ID
     * @return the response stream with an incremented reference count
     * @throws ResourceNotFoundException if the task is not registered or has already completed
     */
    public FetchPhaseResponseStream acquireResponseStream(long coordinatingTaskId, ShardId shardId) {
        final var outerRequest = tasks.get(new ResponseStreamKey(coordinatingTaskId, shardId));
        if (outerRequest == null || outerRequest.tryIncRef() == false) {
            throw new ResourceNotFoundException("fetch task [" + coordinatingTaskId + "] not found");
        }
        return outerRequest;
    }
}
