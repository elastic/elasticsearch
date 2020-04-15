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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class RequestTracker {

    private final Map<Long, ListenableFuture<Void>> ongoingRequests = new HashMap<>();
    private final LocalCheckpointTracker checkpointTracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);

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
                    synchronized (RequestTracker.this) {
                        ongoingRequests.remove(requestSeqNo);
                    }
                    listener.onResponse(v);
                }

                @Override
                public void onFailure(Exception e) {
                    // We do not remote the future to cache the error for retried requests
                    listener.onFailure(e);
                }
            }, EsExecutors.newDirectExecutorService());
            return future;
        }
    }
}
