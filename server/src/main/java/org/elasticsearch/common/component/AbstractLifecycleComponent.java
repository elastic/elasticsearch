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

/**
 * A component with a {@link Lifecycle} which is used to link its start and stop activities to those of the Elasticsearch node which
 * contains it.
 */
public abstract class AbstractLifecycleComponent implements LifecycleComponent {

    protected final Lifecycle lifecycle = new Lifecycle();

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Initialize this component. Other components may not yet exist when this constructor is called.
     */
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
    public final void start() {
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

    /**
     * Start this component. Typically that means doing things like launching background processes and registering listeners on other
     * components. Other components have been initialized by this point, but may not yet be started.
     * <p>
     * If this method throws an exception then the startup process will fail, but this component will not be stopped before it is closed.
     * <p>
     * This method is called while synchronized on {@link #lifecycle}. It is only called once in the lifetime of a component, although it
     * may not be called at all if the startup process encountered some kind of fatal error, such as the failure of some other component to
     * initialize or start.
     */
    protected abstract void doStart();

    @Override
    public final void stop() {
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

    /**
     * Stop this component. Typically that means doing the reverse of whatever {@link #doStart} does.
     * <p>
     * This method is called while synchronized on {@link #lifecycle}. It is only called once in the lifetime of a component, after calling
     * {@link #doStart}, although it will not be called at all if this component did not successfully start.
     */
    protected abstract void doStop();

    @Override
    public final void close() {
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

    /**
     * Close this component. Typically that means doing the reverse of whatever happened during initialization, such as releasing resources
     * acquired there.
     * <p>
     * This method is called while synchronized on {@link #lifecycle}. It is called once in the lifetime of a component. If the component
     * was started then it will be stopped before it is closed, and once it is closed it will not be started or stopped.
     */
    protected abstract void doClose() throws IOException;
}
