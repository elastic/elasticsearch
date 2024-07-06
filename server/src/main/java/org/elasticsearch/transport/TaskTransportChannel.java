/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Releasable;

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
        try {
            channel.sendResponse(response);
        } finally {
            onTaskFinished.close();
        }
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
        return Strings.format("TaskTransportChannel{task=%d}{%s}", taskId, channel);
    }
}
