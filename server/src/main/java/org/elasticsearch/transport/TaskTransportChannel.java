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
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskTransportChannel implements TransportChannel {

    private final long taskId;
    private final TransportChannel channel;
    private final ReleaseAfter onTaskFinished;

    TaskTransportChannel(long taskId, TransportChannel channel, Releasable onTaskFinished) {
        this.taskId = taskId;
        this.channel = channel;
        this.onTaskFinished = wrapAsReleaseAfter(onTaskFinished);
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            channel.sendResponse(response);
        } finally {
            onTaskFinished.release(false);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            channel.sendResponse(exception);
        } finally {
            onTaskFinished.release(true);
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

    private ReleaseAfter wrapAsReleaseAfter(Releasable releasable) {
        if (Assertions.ENABLED) {
            return new AssertingReleaseAfter(releasable);
        } else {
            return ignored -> releasable.close();
        }
    }

    /**
     * Allowing the release method to be called only once, or twice with the isExceptionResponse parameter is false, followed by true.
     */
    private interface ReleaseAfter {
        void release(boolean isExceptionResponse);
    }

    private static class AssertingReleaseAfter implements ReleaseAfter {
        private volatile Exception releaseByOnFailure;
        private volatile Exception releaseByOnResponse;
        private final Releasable releasable;
        private final AtomicBoolean released = new AtomicBoolean();

        AssertingReleaseAfter(Releasable releasable) {
            this.releasable = releasable;
        }

        @Override
        public void release(boolean isExceptionResponse) {
            assert releaseByOnFailure == null : new AssertionError("already released on failure", releaseByOnFailure);
            if (isExceptionResponse) {
                releaseByOnFailure = new Exception();
            } else {
                assert releaseByOnResponse == null : new AssertionError("already released on response", releaseByOnResponse);
                releaseByOnResponse = new Exception();
            }
            if (released.compareAndSet(false, true)) {
                releasable.close();
            }
        }
    }
}
