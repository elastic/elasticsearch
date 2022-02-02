/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractLifecycleComponent implements LifecycleComponent {

    protected final Lifecycle lifecycle = new Lifecycle();

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    protected AbstractLifecycleComponent() {}

    @Override
    public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void start() {
        synchronized (lifecycle) {
            if (lifecycle.canMoveToStarted() == false) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeStart();
            }
            doStart();
            lifecycle.moveToStarted();
            for (LifecycleListener listener : listeners) {
                listener.afterStart();
            }
        }
    }

    protected abstract void doStart();

    @Override
    public void stop() {
        synchronized (lifecycle) {
            if (lifecycle.canMoveToStopped() == false) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeStop();
            }
            lifecycle.moveToStopped();
            doStop();
            for (LifecycleListener listener : listeners) {
                listener.afterStop();
            }
        }
    }

    protected abstract void doStop();

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (lifecycle.canMoveToClosed() == false) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeClose();
            }
            lifecycle.moveToClosed();
            try {
                doClose();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                for (LifecycleListener listener : listeners) {
                    listener.afterClose();
                }
            }
        }
    }

    protected abstract void doClose() throws IOException;
}
