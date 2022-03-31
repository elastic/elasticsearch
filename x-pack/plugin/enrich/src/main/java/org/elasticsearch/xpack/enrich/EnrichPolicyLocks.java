/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A coordination object that allows multiple distinct polices to be executed concurrently, but also makes sure that a single
 * policy can only have one execution in flight at a time. Additionally, this class allows for capturing the current execution
 * state of any policy executions in flight. This execution state can be captured and then later be used to verify that no policy
 * executions have started in the time between the first state capturing.
 */
public class EnrichPolicyLocks {

    /**
     * A snapshot in time detailing if any policy executions are in flight and total number of local executions that
     * have been kicked off since the node has started
     */
    public static class EnrichPolicyExecutionState {
        final boolean anyPolicyInFlight;
        final long executions;

        EnrichPolicyExecutionState(boolean anyPolicyInFlight, long executions) {
            this.anyPolicyInFlight = anyPolicyInFlight;
            this.executions = executions;
        }

        public boolean isAnyPolicyInFlight() {
            return anyPolicyInFlight;
        }
    }

    /**
     * A read-write lock that allows for policies to be executed concurrently with minimal overhead, but allows for blocking
     * policy locking operations while capturing the state of policy executions.
     */
    private final ReadWriteLock currentStateLock = new ReentrantReadWriteLock(true);

    /**
     * A mapping of policy name to a semaphore used for ensuring that a single policy can only have one execution in flight
     * at a time.
     */
    private final ConcurrentHashMap<String, Semaphore> policyLocks = new ConcurrentHashMap<>();

    /**
     * A counter that is used as a sort of policy execution sequence id / dirty bit. This is incremented every time a policy
     * successfully acquires an execution lock.
     */
    private final AtomicLong policyRunCounter = new AtomicLong(0L);

    /**
     * Locks a policy to prevent concurrent execution. If the policy is currently executing, this method will immediately
     * throw without waiting. This method only blocks if another thread is currently capturing the current policy execution state.
     * @param policyName The policy name to lock for execution
     * @throws EsRejectedExecutionException if the policy is locked already or if the maximum number of concurrent policy executions
     *                                      has been reached
     */
    public void lockPolicy(String policyName) {
        currentStateLock.readLock().lock();
        try {
            Semaphore runLock = policyLocks.computeIfAbsent(policyName, (name) -> new Semaphore(1));
            boolean acquired = runLock.tryAcquire();
            if (acquired == false) {
                throw new EsRejectedExecutionException(
                    "Could not obtain lock because policy execution for [" + policyName + "] is already in progress."
                );
            }
            policyRunCounter.incrementAndGet();
        } finally {
            currentStateLock.readLock().unlock();
        }
    }

    /**
     * Captures a snapshot of the current policy execution state. This method never blocks, instead assuming that a policy is
     * currently starting its execution and returns an appropriate state.
     * @return The current state of in-flight policy executions
     */
    public EnrichPolicyExecutionState captureExecutionState() {
        if (currentStateLock.writeLock().tryLock()) {
            try {
                long revision = policyRunCounter.get();
                long currentPolicyExecutions = policyLocks.mappingCount();
                return new EnrichPolicyExecutionState(currentPolicyExecutions > 0L, revision);
            } finally {
                currentStateLock.writeLock().unlock();
            }
        }
        return new EnrichPolicyExecutionState(true, policyRunCounter.get());
    }

    /**
     * Checks if the current execution state matches that of the given execution state. Used to ensure that over a period of time
     * no changes to the policy execution state have occurred.
     * @param previousState The previous state to check the current state against
     * @return true if the current state matches the given previous state, false if policy executions have changed over time.
     */
    boolean isSameState(EnrichPolicyExecutionState previousState) {
        EnrichPolicyExecutionState currentState = captureExecutionState();
        return currentState.anyPolicyInFlight == previousState.anyPolicyInFlight && currentState.executions == previousState.executions;
    }

    /**
     * Releases the lock for a given policy name, allowing it to be executed.
     * @param policyName The policy to release.
     */
    public void releasePolicy(String policyName) {
        currentStateLock.readLock().lock();
        try {
            policyLocks.remove(policyName);
        } finally {
            currentStateLock.readLock().unlock();
        }
    }
}
