/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        return "SyncedFlushRequest{" + "indices=" + Arrays.toString(indices) + "}";
    }
}
