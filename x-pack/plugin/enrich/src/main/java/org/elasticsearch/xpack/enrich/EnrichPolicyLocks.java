package org.elasticsearch.xpack.enrich;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

class EnrichPolicyLocks {

    private final ReadWriteLock coordinationLock = new ReentrantReadWriteLock(true);
    private final ConcurrentHashMap<String, Semaphore> policyLocks = new ConcurrentHashMap<>();
    private final AtomicLong policyRunCounter = new AtomicLong(0L);

    static class LockState {
        final boolean runningPolicies;
        final long revision;

        LockState(boolean safe, long revision) {
            this.runningPolicies = safe;
            this.revision = revision;
        }
    }

    void lockPolicy(String policyName) {
        coordinationLock.readLock().lock();
        try {
            Semaphore runLock = policyLocks.computeIfAbsent(policyName, (name) -> new Semaphore(1));
            boolean acquired = runLock.tryAcquire();
            if (acquired == false) {
                throw new EsRejectedExecutionException("Policy execution failed. Policy execution for [" + policyName +
                    "] is already in progress.");
            }
            policyRunCounter.incrementAndGet();
        } finally {
            coordinationLock.readLock().unlock();
        }
    }

    LockState lockState() {
        if (coordinationLock.writeLock().tryLock()) {
            try {
                long revision = policyRunCounter.get();
                int currentPolicyExecutions = policyLocks.size();
                return new LockState(currentPolicyExecutions > 0, revision);
            } finally {
                coordinationLock.writeLock().unlock();
            }
        }
        return new LockState(false, policyRunCounter.get());
    }

    boolean isSafe(LockState previousState) {
        if (previousState.runningPolicies == true) {
            return false;
        }
        LockState currentState = lockState();
        return currentState.runningPolicies == false && currentState.revision == previousState.revision;
    }

    void releasePolicy(String policyName) {
        coordinationLock.readLock().lock();
        try {
            policyLocks.remove(policyName);
        } finally {
            coordinationLock.readLock().unlock();
        }
    }
}
