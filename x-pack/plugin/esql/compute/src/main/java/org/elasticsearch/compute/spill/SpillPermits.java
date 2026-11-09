/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounds how many pages are detached for serialization at once, per query or per node. A page handed to a codec task
 * stays in memory until the task has appended it, so without a bound a query could detach its whole working set while the
 * codec pool is busy. Acquisition never blocks: a caller that gets no permit is called back when one is released, in
 * arrival order, from the releasing thread.
 */
public final class SpillPermits {

    private final int max;
    private final ArrayDeque<ActionListener<Releasable>> waiters = new ArrayDeque<>();
    private int inUse;

    public SpillPermits(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("permits must be positive, got [" + max + "]");
        }
        this.max = max;
    }

    /**
     * Calls the listener with a permit, now if one is free, otherwise when one is released. Closing the permit releases it;
     * closing it twice is harmless.
     */
    public void acquire(ActionListener<Releasable> listener) {
        synchronized (this) {
            if (inUse >= max) {
                waiters.add(listener);
                return;
            }
            inUse++;
        }
        listener.onResponse(new Permit());
    }

    /**
     * Hands the permit to the oldest waiter, on the releasing thread. A waiter that closes its permit inline hands it on
     * from inside this call, so a chain of such waiters recurses once per link; waiters are few, bounded by the tasks a
     * query may have queued. A waiter whose callback throws, which the listener contract forbids, gets the error back
     * through its own failure path and the permit moves on; the releasing caller is never affected.
     */
    private void release() {
        ActionListener<Releasable> next;
        synchronized (this) {
            next = waiters.poll();
            if (next == null) {
                inUse--;
                return;
            }
        }
        Permit permit = new Permit();
        try {
            next.onResponse(permit);
        } catch (RuntimeException e) {
            try {
                permit.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            next.onFailure(e);
            assert false : e;
        }
    }

    public synchronized int inUse() {
        return inUse;
    }

    public synchronized int waiting() {
        return waiters.size();
    }

    private final class Permit implements Releasable {
        private final AtomicBoolean released = new AtomicBoolean();

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
