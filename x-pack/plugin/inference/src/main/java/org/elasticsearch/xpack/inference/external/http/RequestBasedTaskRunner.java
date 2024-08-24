/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Runs a command synchronously on at most one thread.  Threads make a request to run the command.  If no thread is running the command,
 * then the command will start on the provided {@link #threadPool}'s {@link #executorServiceName}.  If a thread is currently running the
 * command, then that thread is notified to rerun the command after it is finished.</p>
 *
 * <p>This guarantees only one thread is working on a command at a given point in time.</p>
 */
class RequestBasedTaskRunner {
    private final Runnable command;
    private final ThreadPool threadPool;
    private final String executorServiceName;
    private final StateMachine stateMachine = new StateMachine();

    RequestBasedTaskRunner(Runnable command, ThreadPool threadPool, String executorServiceName) {
        this.command = Objects.requireNonNull(command);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executorServiceName = Objects.requireNonNull(executorServiceName);
    }

    /**
     * If there is currently a thread running in a loop, it should pick up this new request.
     * If not, check if this thread is one of ours and reuse it.
     * Else, offload to a new thread so we do not block another threadpool's thread.
     */
    public void requestNextRun() {
        if (stateMachine.tryStart()) {
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread().getName());
            if (executorServiceName.equalsIgnoreCase(currentThreadPool)) {
                run();
            } else {
                threadPool.executor(executorServiceName).execute(this::run);
            }
        }
    }

    public void cancel() {
        stateMachine.cancel();
    }

    private void run() {
        do {
            command.run();
        } while (stateMachine.tryStop() == false);
    }

    private static class StateMachine {
        private final int STOPPED = 0, STARTED = 1, CANCELED = 2;
        private int rerunCount = 0;
        private final AtomicInteger state = new AtomicInteger(STOPPED);

        /**
         * If we are currently CANCELED, stay in CANCELED and return false.
         * If we are currently STOPPED, move to STARTED and return true.
         * If we are currently STARTED, stay in STARTED, increment the rerun count, and return false.
         */
        private boolean tryStart() {
            var previousState = state.getAndUpdate(currentState -> {
                return switch (currentState) {
                    case CANCELED -> CANCELED;
                    case STARTED -> {
                        rerunCount += 1;
                        yield STARTED;
                    }
                    default -> STARTED;
                };
            });
            return previousState == STOPPED;
        }

        /**
         * If we are currently CANCELED, stay in CANCELED and return true.
         * If we are currently STARTED, and there are reruns, stay in STARTED and return false.
         * If we are currently STARTED, and there are no reruns, move to STOPPED and return true.
         * If we are currently STOPPED, stay in STOPPED and return true.
         */
        private boolean tryStop() {
            var nextState = state.updateAndGet(currentState -> {
                return switch (currentState) {
                    case CANCELED -> CANCELED;
                    case STARTED -> {
                        if (rerunCount > 0) {
                            rerunCount -= 1;
                            yield STARTED;
                        }
                        yield STOPPED;
                    }
                    default -> STOPPED;
                };
            });
            return nextState != STARTED;
        }

        private void cancel() {
            state.updateAndGet(currentState -> {
                rerunCount = 0;
                return CANCELED;
            });
        }
    }
}
