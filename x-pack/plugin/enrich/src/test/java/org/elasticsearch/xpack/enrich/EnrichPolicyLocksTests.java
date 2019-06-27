package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class EnrichPolicyLocksTests extends ESTestCase {

    public void testLockPolicy() {
        EnrichPolicyLocks policyLocks = new EnrichPolicyLocks();
        String policy1 = "policy1";
        String policy2 = "policy2";

        // Lock
        policyLocks.lockPolicy(policy1);

        // Ensure that locked policies are rejected
        EsRejectedExecutionException exception1 = expectThrows(EsRejectedExecutionException.class,
            () -> policyLocks.lockPolicy(policy1));
        assertThat(exception1.getMessage(), is(equalTo("Policy execution failed. Policy execution for [policy1]" +
            " is already in progress.")));

        policyLocks.lockPolicy(policy2);
        EsRejectedExecutionException exception2 = expectThrows(EsRejectedExecutionException.class,
            () -> policyLocks.lockPolicy(policy2));

        assertThat(exception2.getMessage(), is(equalTo("Policy execution failed. Policy execution for [policy2]" +
            " is already in progress.")));
    }

    public void testSafePoint() {
        EnrichPolicyLocks policyLocks = new EnrichPolicyLocks();
        String policy = "policy";
        EnrichPolicyLocks.LockState lockState;

        // Get a safe point - should note as safe and revision 1 since nothing has happened yet
        lockState = policyLocks.lockState();
        assertThat(lockState.runningPolicies, is(true));
        assertThat(lockState.revision, is(0L));
        assertThat(policyLocks.isSafe(lockState), is(true));

        // Get another safe point - should still note as safe and revision 1 since nothing has happened yet
        lockState = policyLocks.lockState();
        assertThat(lockState.runningPolicies, is(true));
        assertThat(lockState.revision, is(0L));
        assertThat(policyLocks.isSafe(lockState), is(true));

        // Lock a policy and leave it open (a
        policyLocks.lockPolicy(policy);

        // Get a third safe point - should have a new revision and report unsafe since execution is in progress
        lockState = policyLocks.lockState();
        assertThat(lockState.runningPolicies, is(false));
        assertThat(lockState.revision, is(1L));
        assertThat(policyLocks.isSafe(lockState), is(false));

        // Unlock the policy
        policyLocks.releasePolicy(policy);

        // Get a fourth safe point - should have the same revision as third, and report safe since the previous execution is complete
        lockState = policyLocks.lockState();
        assertThat(lockState.runningPolicies, is(false));
        assertThat(lockState.revision, is(1L));
        assertThat(policyLocks.isSafe(lockState), is(false));

        // Create a fifth safe point, lock and release a policy, and check if the safe point is still safe
        lockState = policyLocks.lockState();
        assertThat(lockState.runningPolicies, is(false));
        assertThat(lockState.revision, is(1L));
        policyLocks.lockPolicy(policy);
        policyLocks.releasePolicy(policy);
        // Should report as unsafe as there was a transient "policy execution" between getting the safe point and checking it.
        assertThat(policyLocks.isSafe(lockState), is(false));
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
