/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A refresh request making all operations performed since the last refresh available for search. The (near) real-time
 * capabilities depends on the index engine used. For example, the internal one requires refresh to be called, but by
 * default a refresh is scheduled periodically.
 *
 * @see org.elasticsearch.client.internal.Requests#refreshRequest(String...)
 * @see org.elasticsearch.client.internal.IndicesAdminClient#refresh(RefreshRequest)
 * @see RefreshResponse
 */
public class RefreshRequest extends BroadcastRequest<RefreshRequest> {

    private boolean waitIfOngoing = true;

    public RefreshRequest(String... indices) {
        super(indices);
    }

    public RefreshRequest(StreamInput in) throws IOException {
        super(in);
        waitIfOngoing = in.readBoolean();
    }

    /**
     * Returns {@code true} iff a flush should block
     * if a another flush operation is already running. Otherwise {@code false}
     */
    public boolean waitIfOngoing() {
        return this.waitIfOngoing;
    }

    /**
     * if set to {@code true} the flush will block
     * if a another flush operation is already running until the flush can be performed.
     * The default is <code>true</code>
     */
    public RefreshRequest waitIfOngoing(boolean waitIfOngoing) {
        this.waitIfOngoing = waitIfOngoing;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(waitIfOngoing);
    }

    @Override
    public String toString() {
        return "RefreshRequest{" + "waitIfOngoing=" + waitIfOngoing + "}";
    }
}
