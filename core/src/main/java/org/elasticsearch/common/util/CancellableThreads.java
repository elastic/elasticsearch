/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util;

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility class for multi threaded operation that needs to be cancellable via interrupts. Every cancellable operation should be
 * executed via {@link #execute(Interruptable)}, which will capture the executing thread and make sure it is interrupted in the case
 * cancellation.
 */
public class CancellableThreads {
    private final Set<Thread> threads = new HashSet<>();
    private boolean cancelled = false;
    private String reason;

    public synchronized boolean isCancelled() {
        return cancelled;
    }


    /** call this will throw an exception if operation was cancelled. Override {@link #onCancel(String, java.lang.Throwable)} for custom failure logic */
    public synchronized void checkForCancel() {
        if (isCancelled()) {
            onCancel(reason, null);
        }
    }

    /**
     * called if {@link #checkForCancel()} was invoked after the operation was cancelled.
     * the default implementation always throws an {@link ExecutionCancelledException}, suppressing
     * any other exception that occurred before cancellation
     *
     * @param reason              reason for failure supplied by the caller of {@link #cancel}
     * @param suppressedException any error that was encountered during the execution before the operation was cancelled.
     */
    protected void onCancel(String reason, @Nullable Throwable suppressedException) {
        RuntimeException e = new ExecutionCancelledException("operation was cancelled reason [" + reason + "]");
        if (suppressedException != null) {
            e.addSuppressed(suppressedException);
        }
        throw e;
    }

    private synchronized boolean add() {
        checkForCancel();
        threads.add(Thread.currentThread());
        // capture and clean the interrupted thread before we start, so we can identify
        // our own interrupt. we do so under lock so we know we don't clear our own.
        return Thread.interrupted();
    }

    /**
     * run the Interruptable, capturing the executing thread. Concurrent calls to {@link #cancel(String)} will interrupt this thread
     * causing the call to prematurely return.
     *
     * @param interruptable code to run
     */
    public void execute(Interruptable interruptable) {
        try {
            executeIO(interruptable);
        } catch (IOException e) {
            assert false : "the passed interruptable can not result in an IOException";
            throw new RuntimeException("unexpected IO exception", e);
        }
    }
    /**
     * run the Interruptable, capturing the executing thread. Concurrent calls to {@link #cancel(String)} will interrupt this thread
     * causing the call to prematurely return.
     *
     * @param interruptable code to run
     */
    public void executeIO(IOInterruptable interruptable) throws IOException {
        boolean wasInterrupted = add();
        RuntimeException runtimeException = null;
        IOException ioException = null;

        try {
            interruptable.run();
        } catch (InterruptedException | ThreadInterruptedException e) {
            // assume this is us and ignore
        } catch (RuntimeException t) {
            runtimeException = t;
        } catch (IOException e) {
            ioException = e;
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
        synchronized (this) {
            if (isCancelled()) {
                onCancel(reason, ioException != null ? ioException : runtimeException);
            } else if (ioException != null) {
                // if we're not canceling, we throw the original exception
                throw ioException;
            }
            if (runtimeException != null) {
                // if we're not canceling, we throw the original exception
                throw runtimeException;
            }
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


    public interface Interruptable extends IOInterruptable {
        void run() throws InterruptedException;
    }

    public interface IOInterruptable {
        void run() throws IOException, InterruptedException;
    }

    public static class ExecutionCancelledException extends ElasticsearchException {

        public ExecutionCancelledException(String msg) {
            super(msg);
        }

        public ExecutionCancelledException(StreamInput in) throws IOException {
            super(in);
        }
    }
}
