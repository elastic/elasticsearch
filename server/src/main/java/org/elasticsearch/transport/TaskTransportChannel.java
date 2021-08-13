/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

public class TaskTransportChannel implements TransportChannel {

    private final TransportChannel channel;
    private final Releasable onTaskFinished;

    TaskTransportChannel(TransportChannel channel, Releasable onTaskFinished) {
        this.channel = channel;
        this.onTaskFinished = onTaskFinished;
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public String getChannelType() {
        return channel.getChannelType();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            onTaskFinished.close();
        } finally {
            channel.sendResponse(response);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            onTaskFinished.close();
        } finally {
            channel.sendResponse(exception);
        }
    }

    @Override
    public Version getVersion() {
        return channel.getVersion();
    }

    public TransportChannel getChannel() {
        return channel;
    }
}
