/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
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
                existingFuture.addListener(listener, EsExecutors.newDirectExecutorService());
            } else {
                listener.onResponse(null);
            }
            return null;
        } else {
            checkpointTracker.markSeqNoAsProcessed(requestSeqNo);
            final ListenableFuture<Void> future = new ListenableFuture<>();
            ongoingRequests.put(requestSeqNo, future);
            future.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    ongoingRequests.remove(requestSeqNo);
                    listener.onResponse(v);
                }

                @Override
                public void onFailure(Exception e) {
                    // We do not remove the future to cache the error for retried requests
                    listener.onFailure(e);
                }
            }, EsExecutors.newDirectExecutorService());
            return future;
        }
    }
}
