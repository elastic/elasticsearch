/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * A coordination object that allows multiple distinct polices to be executed concurrently, but also makes sure that a single
 * policy can only have one execution in flight at a time. Additionally, this class allows for capturing the current execution
 * state of any policy executions in flight. This execution state can be captured and then later be used to verify that no policy
 * executions have started in the time between the first state capturing.
 */
public class EnrichPolicyLocks {

    /**
     * An instance of a specific lock on a single policy object. Ensures that when unlocking a policy, the policy is only unlocked if this
     * object is the owner of the held lock. Additionally, this manages the lock lifecycle for any other resources tracked by the policy
     * coordination logic, such as a policy execution's target index.
     */
    public class EnrichPolicyLock implements Releasable {
        private final String policyName;
        private final String enrichIndexName;
        private final Semaphore executionLease;

        private EnrichPolicyLock(String policyName, String enrichIndexName, Semaphore executionLease) {
            this.policyName = policyName;
            this.enrichIndexName = enrichIndexName;
            this.executionLease = executionLease;
        }

        /**
         * Unlocks this policy for execution and maintenance IFF this lock represents the currently held semaphore for a policy name. If
         * this lock was created for an execution, the target index for the policy execution is also cleared from the locked state.
         */
        @Override
        public void close() {
            if (enrichIndexName != null) {
                boolean wasRemoved = workingIndices.remove(enrichIndexName, executionLease);
                assert wasRemoved
                    : "Target index [" + enrichIndexName + "] for policy [" + policyName + "] was removed prior to policy unlock";
            }
            boolean wasRemoved = policyLocks.remove(policyName, executionLease);
            assert wasRemoved : "Second attempt was made to unlock policy [" + policyName + "]";
        }
    }

    /**
     * A mapping of policy name to a semaphore used for ensuring that a single policy can only have one execution in flight
     * at a time.
     */
    private final ConcurrentHashMap<String, Semaphore> policyLocks = new ConcurrentHashMap<>();

    /**
     * When a policy is locked for execution the new index that is created is added to this set to keep it from being accidentally
     * cleaned up by the maintenance task.
     */
    private final ConcurrentHashMap<String, Semaphore> workingIndices = new ConcurrentHashMap<>();

    /**
     * Locks a policy to prevent concurrent execution. If the policy is currently executing, this method will immediately
     * throw without waiting. This method only blocks if another thread is currently capturing the current policy execution state.
     * <br/><br/>
     * If a policy is being executed, use {@link EnrichPolicyLocks#lockPolicy(String, String)} instead in order to properly track the
     * new enrich index that will be created.
     * @param policyName The policy name to lock for execution
     * @throws EsRejectedExecutionException if the policy is locked already or if the maximum number of concurrent policy executions
     *                                      has been reached
     */
    public EnrichPolicyLock lockPolicy(String policyName) {
        return lockPolicy(policyName, null);
    }

    /**
     * Locks a policy to prevent concurrent execution. If the policy is currently executing, this method will immediately
     * throw without waiting. This method only blocks if another thread is currently capturing the current policy execution state.
     * <br/><br/>
     * If a policy needs to be locked just to ensure it is not executing, use {@link EnrichPolicyLocks#lockPolicy(String)} instead since
     * no new enrich indices need to be maintained.
     * @param policyName The policy name to lock for execution
     * @param enrichIndexName If the policy is being executed, this parameter denotes the index that should be protected from maintenance
     *                  operations.
     * @throws EsRejectedExecutionException if the policy is locked already or if the maximum number of concurrent policy executions
     *                                      has been reached
     */
    public EnrichPolicyLock lockPolicy(String policyName, String enrichIndexName) {
        Semaphore runLock = policyLocks.computeIfAbsent(policyName, (name) -> new Semaphore(1));
        boolean acquired = runLock.tryAcquire();
        if (acquired == false) {
            throw new EsRejectedExecutionException(
                "Could not obtain lock because policy execution for [" + policyName + "] is already in progress."
            );
        }
        if (enrichIndexName != null) {
            Semaphore previous = workingIndices.putIfAbsent(enrichIndexName, runLock);
            assert previous == null : "Target index [" + enrichIndexName + "] is already claimed by an execution, or was not cleaned up.";
        }
        return new EnrichPolicyLock(policyName, enrichIndexName, runLock);
    }

    public Set<String> lockedPolices() {
        // Wrap as unmodifiable instead of copying
        return Collections.unmodifiableSet(policyLocks.keySet());
    }

    public Set<String> inflightPolicyIndices() {
        // Wrap as unmodifiable instead of copying
        return Collections.unmodifiableSet(workingIndices.keySet());
    }

}
