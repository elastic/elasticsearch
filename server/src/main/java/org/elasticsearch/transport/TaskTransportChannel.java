/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public class TaskTransportChannel implements TransportChannel {

    private final long taskId;
    private final TransportChannel channel;
    private final Releasable onTaskFinished;

    TaskTransportChannel(long taskId, TransportChannel channel, Releasable onTaskFinished) {
        this.taskId = taskId;
        this.channel = channel;
        this.onTaskFinished = onTaskFinished;
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public void sendResponse(TransportResponse response) {
        // Delegate to the underlying channel with onTaskFinished as the releasable
        // This ensures onTaskFinished is called after the response is sent
        channel.sendResponse(response, onTaskFinished);
    }

    @Override
    public void sendResponse(TransportResponse response, Releasable onSendComplete) {
        // Combine both releasables and delegate to the underlying channel
        Releasable combined = Releasables.wrap(onTaskFinished, onSendComplete);
        channel.sendResponse(response, combined);
    }

    @Override
    public void sendResponse(Exception exception) {
        try {
            channel.sendResponse(exception);
        } finally {
            onTaskFinished.close();
        }
    }

    @Override
    public TransportVersion getVersion() {
        return channel.getVersion();
    }

    public TransportChannel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "TaskTransportChannel{task=" + taskId + "}{" + channel + "}";
    }
}
