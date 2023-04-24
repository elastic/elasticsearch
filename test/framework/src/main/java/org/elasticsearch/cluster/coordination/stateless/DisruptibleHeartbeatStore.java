/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.action.ActionListener;

import java.io.IOException;

public abstract class DisruptibleHeartbeatStore implements HeartbeatStore {
    private final HeartbeatStore delegate;

    protected DisruptibleHeartbeatStore(HeartbeatStore delegate) {
        this.delegate = delegate;
    }

    protected abstract boolean isDisrupted();

    @Override
    public final void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
        if (isDisrupted()) {
            listener.onFailure(new IOException("simulating disrupted access to shared store"));
        } else {
            delegate.writeHeartbeat(newHeartbeat, listener);
        }
    }

    @Override
    public final void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
        if (isDisrupted()) {
            listener.onFailure(new IOException("simulating disrupted access to shared store"));
        } else {
            delegate.readLatestHeartbeat(listener);
        }
    }
}
