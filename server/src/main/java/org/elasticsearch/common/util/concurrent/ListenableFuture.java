/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A future implementation that allows for the result to be passed to listeners waiting for
 * notification. This is useful for cases where a computation is requested many times
 * concurrently, but really only needs to be performed a single time. Once the computation
 * has been performed the registered listeners will be notified. If the computation has already
 * been performed, a request to add a listener will simply result in execution of the listener
 * on the calling thread.
 */
public final class ListenableFuture<V> extends BaseFuture<V> implements ActionListener<V> {

    private volatile boolean done = false;
    private List<ActionListener<V>> listeners;

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be
     * notified of a response or exception on the thread completing this future.
     * If the future has completed, the listener will be notified immediately without forking to
     * a different thread.
     */
    public void addListener(ActionListener<V> listener) {
        if (done) {
            // run the callback directly, we don't hold the lock and don't need to fork!
            notifyListenerDirectly(listener);
        } else {
            final boolean run;
            // check done under lock since it could have been modified and protect modifications
            // to the list under lock
            synchronized (this) {
                if (done) {
                    run = true;
                } else {
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(listener);
                    run = false;
                }
            }

            if (run) {
                // run the callback directly, we don't hold the lock and don't need to fork!
                notifyListenerDirectly(listener);
            }
        }
    }

    @Override
    protected void done(boolean ignored) {
        final List<ActionListener<V>> existingListeners;
        synchronized (this) {
            done = true;
            existingListeners = listeners;
            if (existingListeners == null) {
                return;
            }
            listeners = null;
        }
        for (ActionListener<V> listener : existingListeners) {
            notifyListenerDirectly(listener);
        }
    }

    private void notifyListenerDirectly(ActionListener<V> listener) {
        try {
            // call get in a non-blocking fashion as we could be on a network thread
            // or another thread like the scheduler, which we should never block!
            assert done;
            listener.onResponse(FutureUtils.get(this, 0L, TimeUnit.NANOSECONDS));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void onResponse(V v) {
        final boolean set = set(v);
        if (set == false) {
            throw new IllegalStateException("did not set value, value or exception already set?");
        }
    }

    @Override
    public void onFailure(Exception e) {
        final boolean set = setException(e);
        if (set == false) {
            throw new IllegalStateException("did not set exception, value already set or exception already set?");
        }
    }
}
