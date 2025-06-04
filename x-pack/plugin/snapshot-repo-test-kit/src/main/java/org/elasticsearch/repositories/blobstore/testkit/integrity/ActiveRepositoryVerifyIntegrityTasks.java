/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exception.ResourceNotFoundException;

import java.util.Map;

/**
 * The repository-verify-integrity tasks that this node is currently coordinating.
 */
class ActiveRepositoryVerifyIntegrityTasks {

    private final Map<Long, RepositoryVerifyIntegrityResponseStream> responseStreamsByCoordinatingTaskId = ConcurrentCollections
        .newConcurrentMap();

    Releasable registerResponseBuilder(long coordinatingTaskId, RepositoryVerifyIntegrityResponseStream responseStream) {
        assert responseStream.hasReferences(); // ref held until the REST-layer listener is completed

        final var previous = responseStreamsByCoordinatingTaskId.putIfAbsent(coordinatingTaskId, responseStream);
        if (previous != null) {
            final var exception = new IllegalStateException("already executing verify task [" + coordinatingTaskId + "]");
            assert false : exception;
            throw exception;
        }

        return Releasables.assertOnce(() -> {
            final var removed = responseStreamsByCoordinatingTaskId.remove(coordinatingTaskId, responseStream);
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
    RepositoryVerifyIntegrityResponseStream acquireResponseStream(long taskId) {
        final var outerRequest = responseStreamsByCoordinatingTaskId.get(taskId);
        if (outerRequest == null || outerRequest.tryIncRef() == false) {
            throw new ResourceNotFoundException("verify task [" + taskId + "] not found");
        }
        return outerRequest;
    }
}
