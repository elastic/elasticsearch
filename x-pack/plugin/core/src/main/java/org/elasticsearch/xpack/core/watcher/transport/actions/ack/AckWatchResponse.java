/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;

/**
 * This class contains the status of the watch. If the watch was successfully acked
 * this will be reflected in the watch status.
 */
public class AckWatchResponse extends ActionResponse {

    private WatchStatus status;

    public AckWatchResponse() {
    }

    public AckWatchResponse(@Nullable WatchStatus status) {
        this.status = status;
    }

    /**
     * @return The watch status
     */
    public WatchStatus getStatus() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readBoolean() ? WatchStatus.read(in) : null;
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
