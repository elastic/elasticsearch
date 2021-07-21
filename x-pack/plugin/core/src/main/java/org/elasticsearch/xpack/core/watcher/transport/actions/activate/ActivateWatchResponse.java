/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.activate;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;

/**
 * This class contains the status of the watch. If the watch was successfully de/activates
 * this will reflected the new state of the watch.
 */
public class ActivateWatchResponse extends ActionResponse {

    private WatchStatus status;

    public ActivateWatchResponse(StreamInput in) throws IOException {
        super(in);
        status = in.readBoolean() ? new WatchStatus(in) : null;
    }

    public ActivateWatchResponse(@Nullable WatchStatus status) {
        this.status = status;
    }

    /**
     * @return The watch status
     */
    public WatchStatus getStatus() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(status != null);
        if (status != null) {
            status.writeTo(out);
        }
    }
}
