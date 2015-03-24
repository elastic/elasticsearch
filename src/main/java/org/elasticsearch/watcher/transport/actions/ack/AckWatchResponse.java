/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This class contains the ackState of the watch, if the watch was successfully acked this will be ACK
 */
public class AckWatchResponse extends ActionResponse {

    private Watch.Status status;

    public AckWatchResponse() {
    }

    public AckWatchResponse(@Nullable Watch.Status status) {
        this.status = status;
    }

    /**
     * @return The ack state for the watch
     */
    public Watch.Status getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readBoolean() ? Watch.Status.read(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(status != null);
        if (status != null) {
            status.writeTo(out);
        }
    }
}
