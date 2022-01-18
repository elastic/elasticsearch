/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility class for multi threaded operation that needs to be cancellable via interrupts. Every cancellable operation should be
 * executed via {@link #execute(Interruptible)}, which will capture the executing thread and make sure it is interrupted in the case
 * of cancellation.
 *
 * Cancellation policy: This class does not support external interruption via <code>Thread#interrupt()</code>. Always use #cancel() instead.
 */
public class CancellableThreads {
    private final Set<Thread> threads = new HashSet<>();
    // needs to be volatile as it is also read outside of synchronized blocks.
    private volatile boolean cancelled = false;
    private final SetOnce<OnCancel> onCancel = new SetOnce<>();
    private String reason;

    public synchronized boolean isCancelled() {
        return cancelled;
    }

    public void checkForCancel() {
        checkForCancel(null);
    }

    private void checkForCancel(Exception beforeCancelException) {
        if (isCancelled()) {
            final String reason;
            final OnCancel onCancel;
            synchronized (this) {
                reason = this.reason;
                onCancel = this.onCancel.get();
            }
            if (onCancel != null) {
                onCancel.onCancel(reason, beforeCancelException);
            }
            // fallback to the default exception
            final RuntimeException cancelExp = new ExecutionCancelledException("operation was cancelled reason [" + reason + "]");
            if (beforeCancelException != null) {
                cancelExp.addSuppressed(beforeCancelException);
            }
            throw cancelExp;
        }
    }

    private synchronized boolean add() {
        checkForCancel();
        threads.add(Thread.currentThread());
        // capture and clean the interrupted thread before we start, so we can identify
        // our own interrupt. we do so under lock so we know we don't clear our own.
        return Thread.interrupted();
    }

    /**
     * run the Interruptible, capturing the executing thread. Concurrent calls to {@link #cancel(String)} will interrupt this thread
     * causing the call to prematurely return.
     *
     * @param interruptible code to run
     */
    public void execute(Interruptible interruptible) {
        boolean wasInterrupted = add();
        boolean cancelledByExternalInterrupt = false;
        RuntimeException runtimeException = null;

        try {
            interruptible.run();
        } catch (InterruptedException | ThreadInterruptedException e) {
            // ignore, this interrupt has been triggered by us in #cancel()...
            assert cancelled : "Interruption via Thread#interrupt() is unsupported. Use CancellableThreads#cancel() instead";
            // we can only reach here if assertions are disabled. If we reach this code and cancelled is false, this means that we've
            // been interrupted externally (which we don't support).
            cancelledByExternalInterrupt = cancelled == false;
        } catch (RuntimeException t) {
            runtimeException = t;
        } finally {
            remove();
        }
        // we are now out of threads collection so we can't be interrupted any more by this class
        // restore old flag and see if we need to fail
        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        } else {
            // clear the flag interrupted flag as we are checking for failure..
            Thread.interrupted();
        }
        checkForCancel(runtimeException);
        if (runtimeException != null) {
            // if we're not canceling, we throw the original exception
            throw runtimeException;
        }
        if (cancelledByExternalInterrupt) {
            // restore interrupt flag to at least adhere to expected behavior
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interruption via Thread#interrupt() is unsupported. Use CancellableThreads#cancel() instead");
        }
    }

    private synchronized void remove() {
        threads.remove(Thread.currentThread());
    }

    /** cancel all current running operations. Future calls to {@link #checkForCancel()} will be failed with the given reason */
    public synchronized void cancel(String reason) {
        if (cancelled) {
            // we were already cancelled, make sure we don't interrupt threads twice
            // this is important in order to make sure that we don't mark
            // Thread.interrupted without handling it
            return;
        }
        cancelled = true;
        this.reason = reason;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        threads.clear();
    }

    public interface Interruptible {
        void run() throws InterruptedException;
    }

    public static class ExecutionCancelledException extends ElasticsearchException {

        public ExecutionCancelledException(String msg) {
            super(msg);
        }

        public ExecutionCancelledException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Registers a callback that will be invoked when some running operations are cancelled or {@link #checkForCancel()} is called.
     */
    public synchronized void setOnCancel(OnCancel onCancel) {
        this.onCancel.set(onCancel);
    }

    @FunctionalInterface
    public interface OnCancel {
        /**
         * Called when some running operations are cancelled or {@link #checkForCancel()} is explicitly called.
         * If this method throws an exception, cancelling tasks will fail with that exception; otherwise they
         * will fail with the default exception {@link ExecutionCancelledException}.
         *
         * @param reason                the reason of the cancellation
         * @param beforeCancelException any error that was encountered during the execution before the operations were cancelled.
         * @see #checkForCancel()
         * @see #setOnCancel(OnCancel)
         */
        void onCancel(String reason, @Nullable Exception beforeCancelException);
    }
}
