/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class RecoveryRequestTracker {

    private final Map<Long, ListenableFuture<Void>> ongoingRequests = Collections.synchronizedMap(new HashMap<>());
    private final LocalCheckpointTracker checkpointTracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);

    /**
     * This method will mark that a request with a unique sequence number has been received. If this is the
     * first time the unique request has been received, this method will return a listener to be completed.
     * The caller should then perform the requested action and complete the returned listener.
     *
     *
     * If the unique request has already been received, this method will either complete the provided listener
     * or attach that listener to the listener returned in the first call. In this case, the method will
     * return null and the caller should not perform the requested action as a prior caller is already
     * performing the action.
     */
    @Nullable
    public synchronized ActionListener<Void> markReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        if (checkpointTracker.hasProcessed(requestSeqNo)) {
            final ListenableFuture<Void> existingFuture = ongoingRequests.get(requestSeqNo);
            if (existingFuture != null) {
                existingFuture.addListener(listener);
            } else {
                listener.onResponse(null);
            }
            return null;
        } else {
            checkpointTracker.markSeqNoAsProcessed(requestSeqNo);
            final ListenableFuture<Void> future = new ListenableFuture<>();
            ongoingRequests.put(requestSeqNo, future);
            future.addListener(listener.delegateFailure((l, v) -> {
                ongoingRequests.remove(requestSeqNo);
                l.onResponse(v);
            }));
            return future;
        }
    }
}
