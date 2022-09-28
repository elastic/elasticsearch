/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class EnrichPolicyLocksTests extends ESTestCase {

    public void testLockPolicy() {
        EnrichPolicyLocks policyLocks = new EnrichPolicyLocks();
        String policy1 = "policy1";
        String policy2 = "policy2";

        // Lock
        policyLocks.lockPolicy(policy1);

        // Ensure that locked policies are rejected
        EsRejectedExecutionException exception1 = expectThrows(EsRejectedExecutionException.class, () -> policyLocks.lockPolicy(policy1));
        assertThat(
            exception1.getMessage(),
            is(equalTo("Could not obtain lock because policy execution for [policy1]" + " is already in progress."))
        );

        policyLocks.lockPolicy(policy2);
        EsRejectedExecutionException exception2 = expectThrows(EsRejectedExecutionException.class, () -> policyLocks.lockPolicy(policy2));

        assertThat(
            exception2.getMessage(),
            is(equalTo("Could not obtain lock because policy execution for [policy2]" + " is already in progress."))
        );
    }

    public void testMaintenanceLocking() throws Exception {
        // Maintenance on fresh locks object returns value without issue.
        final EnrichPolicyLocks locks = new EnrichPolicyLocks();
        Long value = locks.attemptMaintenance(() -> 1L);
        assertThat(value, equalTo(1L));

        // Maintenance on a held policy lock returns no value and no code run.
        locks.lockPolicy("test-policy");
        value = locks.attemptMaintenance(() -> 2L);
        assertThat(value, is(nullValue()));
        // Maintenance on an unheld policy lock returns value without issue.
        locks.releasePolicy("test-policy");
        value = locks.attemptMaintenance(() -> 3L);
        assertThat(value, equalTo(3L));

        // Hold the maintenance lock on another thread with a latch.
        final AtomicLong maintenanceResult = new AtomicLong(0L);
        Thread maintenanceThread = new Thread(() -> {
            Long result;
            do {
                result = locks.attemptMaintenance(maintenanceResult::incrementAndGet);
            } while (result == null);
        });

        // Attempt to lock on the policy in another thread.
        final CountDownLatch policyIsExecuting = new CountDownLatch(1);
        final CountDownLatch policyFinishExecuting = new CountDownLatch(1);
        Thread policyExecutionThread = new Thread(() -> {
            locks.lockPolicy("test-policy");
            try {
                policyIsExecuting.countDown();
                policyFinishExecuting.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                locks.releasePolicy("test-policy");
            }
        });

        // Ensure that maintenance does not advance while the policy is executing
        policyExecutionThread.start(); // Start policy
        policyIsExecuting.await(); // Wait for policy to be locked
        maintenanceThread.start(); // start maintenance while policy is "executing"
        assertBusy(() -> assertThat(maintenanceResult.get(), equalTo(0L))); // Ensure no maintenance progress
        policyFinishExecuting.countDown(); // Finish the policy execution
        policyExecutionThread.join(TimeUnit.SECONDS.toMillis(30)); // Close out the policy thread
        assertThat(policyExecutionThread.isAlive(), is(false));
        maintenanceThread.join(TimeUnit.SECONDS.toMillis(30)); // Close out the maintenance thread
        assertThat(maintenanceThread.isAlive(), is(false));
        assertThat(maintenanceResult.get(), equalTo(1L)); // Ensure maintenance completed
    }

    public void testSafePoint() {
        EnrichPolicyLocks policyLocks = new EnrichPolicyLocks();
        String policy = "policy";
        EnrichPolicyLocks.EnrichPolicyExecutionState executionState;

        // Get exec state - should note as safe and revision 1 since nothing has happened yet
        executionState = policyLocks.captureExecutionState();
        assertThat(executionState.anyPolicyInFlight, is(false));
        assertThat(executionState.executions, is(0L));
        assertThat(policyLocks.isSameState(executionState), is(true));

        // Get another exec state - should still note as safe and revision 1 since nothing has happened yet
        executionState = policyLocks.captureExecutionState();
        assertThat(executionState.anyPolicyInFlight, is(false));
        assertThat(executionState.executions, is(0L));
        assertThat(policyLocks.isSameState(executionState), is(true));

        // Lock a policy and leave it open (a
        policyLocks.lockPolicy(policy);

        // Get a third exec state - should have a new revision and report unsafe since execution is in progress
        executionState = policyLocks.captureExecutionState();
        assertThat(executionState.anyPolicyInFlight, is(true));
        assertThat(executionState.executions, is(1L));

        // Unlock the policy
        policyLocks.releasePolicy(policy);

        // Get a fourth exec state - should have the same revision as third, and report no policies in flight since the previous execution
        // is complete
        executionState = policyLocks.captureExecutionState();
        assertThat(executionState.anyPolicyInFlight, is(false));
        assertThat(executionState.executions, is(1L));

        // Create a fifth exec state, lock and release a policy, and check if the captured exec state is the same as the current state in
        // the lock object
        executionState = policyLocks.captureExecutionState();
        assertThat(executionState.anyPolicyInFlight, is(false));
        assertThat(executionState.executions, is(1L));
        policyLocks.lockPolicy(policy);
        policyLocks.releasePolicy(policy);
        // Should report as not the same as there was a transient "policy execution" between getting the exec state and checking it.
        assertThat(policyLocks.isSameState(executionState), is(false));
    }

    public void testReleasePolicy() {
        EnrichPolicyLocks policyLocks = new EnrichPolicyLocks();
        String policy1 = "policy1";
        String policy2 = "policy2";

        // Lock
        policyLocks.lockPolicy(policy1);
        policyLocks.lockPolicy(policy2);

        // Unlock
        policyLocks.releasePolicy(policy1);
        policyLocks.releasePolicy(policy2);

        // Ensure locking again after release works
        policyLocks.lockPolicy(policy1);
        policyLocks.lockPolicy(policy2);
    }
}
