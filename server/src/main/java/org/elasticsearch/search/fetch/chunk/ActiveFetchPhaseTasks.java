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

import java.util.concurrent.ConcurrentMap;

/**
 * Registers the mapping between coordinating tasks and response streams. When the coordination action starts, it registers the stream.
 * When each chunk arrives, TransportFetchPhaseResponseChunkAction calls acquireResponseStream to find the right response stream.
 */
public final class ActiveFetchPhaseTasks {

    private final ConcurrentMap<Long, FetchPhaseResponseStream> tasks = ConcurrentCollections.newConcurrentMap();

    Releasable registerResponseBuilder(long coordinatingTaskId, FetchPhaseResponseStream responseStream) {
        assert responseStream.hasReferences();

        final var previous = tasks.putIfAbsent(coordinatingTaskId, responseStream);
        if (previous != null) {
            final var exception = new IllegalStateException("already executing verify task [" + coordinatingTaskId + "]");
            assert false : exception;
            throw exception;        }

        return Releasables.assertOnce(() -> {
            final var removed = tasks.remove(coordinatingTaskId, responseStream);
            if (removed == false) {
                final var exception = new IllegalStateException("already completed verify task [" + coordinatingTaskId + "]");
                assert false : exception;
                throw exception;
            }
        });
    }

    /**
     * Obtain the response stream for the given coordinating-node task ID, and increment its refcount.
     * @throws ResourceNotFoundException if the task is not running or its refcount already reached zero (likely because it completed)
     */
    public FetchPhaseResponseStream acquireResponseStream(long coordinatingTaskId) {
        final var outerRequest  = tasks.get(coordinatingTaskId);
        if (outerRequest == null || outerRequest.tryIncRef() == false) {
            throw new ResourceNotFoundException("verify task [" + coordinatingTaskId + "] not found");
        }
        return outerRequest;
    }
}
