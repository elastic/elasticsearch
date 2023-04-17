/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A flush request to flush one or more indices. The flush process of an index basically frees memory from the index
 * by flushing data to the index storage and clearing the internal transaction log. By default, Elasticsearch uses
 * memory heuristics in order to automatically trigger flush operations as required in order to clear memory.
 *
 * @see org.elasticsearch.client.internal.IndicesAdminClient#flush(FlushRequest)
 * @see FlushResponse
 */
public class FlushRequest extends BroadcastRequest<FlushRequest> {

    private boolean force = false;
    private boolean waitIfOngoing = true;

    /**
     * Constructs a new flush request against one or more indices. If nothing is provided, all indices will
     * be flushed.
     */
    public FlushRequest(String... indices) {
        super(indices);
    }

    public FlushRequest(StreamInput in) throws IOException {
        super(in);
        force = in.readBoolean();
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
    public FlushRequest waitIfOngoing(boolean waitIfOngoing) {
        this.waitIfOngoing = waitIfOngoing;
        return this;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public boolean force() {
        return force;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public FlushRequest force(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationError = super.validate();
        if (force && waitIfOngoing == false) {
            validationError = addValidationError("wait_if_ongoing must be true for a force flush", validationError);
        }
        return validationError;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(force);
        out.writeBoolean(waitIfOngoing);
    }

    @Override
    public String toString() {
        return "FlushRequest{" + "waitIfOngoing=" + waitIfOngoing + ", force=" + force + "}";
    }
}
