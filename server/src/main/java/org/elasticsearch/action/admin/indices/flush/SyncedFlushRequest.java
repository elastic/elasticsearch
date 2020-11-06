/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A synced flush request to sync flush one or more indices. The synced flush process of an index performs a flush
 * and writes the same sync id to primary and all copies.
 *
 * <p>Best created with {@link org.elasticsearch.client.Requests#syncedFlushRequest(String...)}. </p>
 *
 * @see org.elasticsearch.client.Requests#flushRequest(String...)
 * @see org.elasticsearch.client.IndicesAdminClient#syncedFlush(SyncedFlushRequest)
 * @see SyncedFlushResponse
 */
public class SyncedFlushRequest extends BroadcastRequest<SyncedFlushRequest> {

    /**
     * Constructs a new synced flush request against one or more indices. If nothing is provided, all indices will
     * be sync flushed.
     */
    public SyncedFlushRequest(String... indices) {
        super(indices);
    }

    public SyncedFlushRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String toString() {
        return "SyncedFlushRequest{" +
            "indices=" + Arrays.toString(indices) + "}";
    }
}
