/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

/**
 * An extension to runnable.
 */
public abstract class AbstractRunnable implements Runnable {

    /**
     * Should the runnable force its execution in case it gets rejected?
     */
    public boolean isForceExecution() {
        return false;
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception t) {
            onFailure(t);
        } finally {
            onAfter();
        }
    }

    /**
     * This method is called in a finally block after successful execution
     * or on a rejection.
     */
    public void onAfter() {
        // nothing by default
    }

    /**
     * This method is invoked for all exception thrown by {@link #doRun()}
     */
    public abstract void onFailure(Exception e);

    /**
     * This should be executed if the thread-pool executing this action rejected the execution.
     * The default implementation forwards to {@link #onFailure(Exception)}
     */
    public void onRejection(Exception e) {
        onFailure(e);
    }

    /**
     * This method has the same semantics as {@link Runnable#run()}
     * @throws InterruptedException if the run method throws an InterruptedException
     */
    protected abstract void doRun() throws Exception;
}
