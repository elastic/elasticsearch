/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.component;

/**
 * Lifecycle state. Allows the following transitions:
 * <ul>
 * <li>INITIALIZED -&gt; STARTED, CLOSED</li>
 * <li>STARTED     -&gt; STOPPED</li>
 * <li>STOPPED     -&gt; CLOSED</li>
 * <li>CLOSED      -&gt; </li>
 * </ul>
 * <p>
 * Also allows to stay in the same state. For example, when calling stop on a component, the
 * following logic can be applied:
 * <pre>
 * public void stop() {
 *  if (lifecycleState.moveToStopped() == false) {
 *      return;
 *  }
 * // continue with stop logic
 * }
 * </pre>
 * <p>
 * NOTE: The Lifecycle class is thread-safe. It is also possible to prevent concurrent state transitions
 * by locking on the Lifecycle object itself. This is typically useful when chaining multiple transitions.
 * <p>
 * Note, closed is only allowed to be called when stopped, so make sure to stop the component first.
 * Here is how the logic can be applied. A lock of the {@code lifecycleState} object is taken so that
 * another thread cannot move the state from {@code STOPPED} to {@code STARTED} before it has moved to
 * {@code CLOSED}.
 * <pre>
 * public void close() {
 *  synchronized (lifecycleState) {
 *      if (lifecycleState.started()) {
 *          stop();
 *      }
 *      if (lifecycleState.moveToClosed() == false) {
 *          return;
 *      }
 *  }
 *  // perform close logic here
 * }
 * </pre>
 */
public final class Lifecycle {

    public enum State {
        INITIALIZED,
        STOPPED,
        STARTED,
        CLOSED
    }

    private volatile State state = State.INITIALIZED;

    public State state() {
        return this.state;
    }

    /**
     * Returns {@code true} if the state is initialized.
     */
    public boolean initialized() {
        return state == State.INITIALIZED;
    }

    /**
     * Returns {@code true} if the state is started.
     */
    public boolean started() {
        return state == State.STARTED;
    }

    /**
     * Returns {@code true} if the state is stopped.
     */
    public boolean stopped() {
        return state == State.STOPPED;
    }

    /**
     * Returns {@code true} if the state is closed.
     */
    public boolean closed() {
        return state == State.CLOSED;
    }

    public boolean stoppedOrClosed() {
        Lifecycle.State state = this.state;
        return state == State.STOPPED || state == State.CLOSED;
    }

    public boolean canMoveToStarted() throws IllegalStateException {
        return switch (state) {
            case INITIALIZED -> true;
            case STARTED -> false;
            case STOPPED -> {
                assert false : "STOPPED -> STARTED";
                throw new IllegalStateException("Can't move to started state when stopped");
            }
            case CLOSED -> {
                assert false : "CLOSED -> STARTED";
                throw new IllegalStateException("Can't move to started state when closed");
            }
        };
    }

    public synchronized boolean moveToStarted() throws IllegalStateException {
        if (canMoveToStarted()) {
            state = State.STARTED;
            return true;
        } else {
            return false;
        }
    }

    public boolean canMoveToStopped() throws IllegalStateException {
        return switch (state) {
            case INITIALIZED -> {
                assert false : "INITIALIZED -> STOPPED";
                throw new IllegalStateException("Can't move to stopped state when not started");
            }
            case STARTED -> true;
            case STOPPED -> false;
            case CLOSED -> {
                assert false : "CLOSED -> STOPPED";
                throw new IllegalStateException("Can't move to stopped state when closed");
            }
        };
    }

    public synchronized boolean moveToStopped() throws IllegalStateException {
        if (canMoveToStopped()) {
            state = State.STOPPED;
            return true;
        } else {
            return false;
        }
    }

    public boolean canMoveToClosed() throws IllegalStateException {
        return switch (state) {
            case INITIALIZED -> true;
            case STARTED -> {
                assert false : "STARTED -> CLOSED";
                throw new IllegalStateException("Can't move directly from STARTED to CLOSED, must move to STOPPED first");
            }
            case STOPPED -> true;
            case CLOSED -> false;
        };
    }

    public synchronized boolean moveToClosed() throws IllegalStateException {
        if (canMoveToClosed()) {
            state = State.CLOSED;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return state.toString();
    }

}
