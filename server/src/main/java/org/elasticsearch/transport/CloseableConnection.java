/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.AbstractRefCounted;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract Transport.Connection that provides common close logic.
 */
public abstract class CloseableConnection extends AbstractRefCounted implements Transport.Connection {

    private final SubscribableListener<Void> closeContext = new SubscribableListener<>();
    private final SubscribableListener<Void> removeContext = new SubscribableListener<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean removed = new AtomicBoolean(false);

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(listener);
    }

    @Override
    public void addRemovedListener(ActionListener<Void> listener) {
        removeContext.addListener(listener);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeContext.onResponse(null);
        }
    }

    public void closeAndFail(Exception e) {
        if (closed.compareAndSet(false, true)) {
            closeContext.onFailure(e);
        }
    }

    @Override
    public void onRemoved() {
        if (removed.compareAndSet(false, true)) {
            removeContext.onResponse(null);
        }
    }

    @Override
    protected void closeInternal() {
        close();
    }
}
