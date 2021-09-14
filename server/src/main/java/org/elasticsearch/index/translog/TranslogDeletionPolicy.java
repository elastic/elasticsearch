/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.util.Counter;
import org.elasticsearch.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TranslogDeletionPolicy {

    private final Map<Object, RuntimeException> openTranslogRef;

    public void assertNoOpenTranslogRefs() {
        if (openTranslogRef.isEmpty() == false) {
            AssertionError e = new AssertionError("not all translog generations have been released");
            openTranslogRef.values().forEach(e::addSuppressed);
            throw e;
        }
    }

    /**
     * Records how many retention locks are held against each
     * translog generation
     */
    private final Map<Long, Counter> translogRefCounts = new HashMap<>();
    private long localCheckpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;


    public TranslogDeletionPolicy() {
        if (Assertions.ENABLED) {
            openTranslogRef = new ConcurrentHashMap<>();
        } else {
            openTranslogRef = null;
        }
    }

    public synchronized void setLocalCheckpointOfSafeCommit(long newCheckpoint) {
        if (newCheckpoint < this.localCheckpointOfSafeCommit) {
            throw new IllegalArgumentException("local checkpoint of the safe commit can't go backwards: " +
                "current [" + this.localCheckpointOfSafeCommit + "] new [" + newCheckpoint + "]");
        }
        this.localCheckpointOfSafeCommit = newCheckpoint;
    }

    /**
     * acquires the basis generation for a new snapshot. Any translog generation above, and including, the returned generation
     * will not be deleted until the returned {@link Releasable} is closed.
     */
    synchronized Releasable acquireTranslogGen(final long translogGen) {
        translogRefCounts.computeIfAbsent(translogGen, l -> Counter.newCounter(false)).addAndGet(1);
        final AtomicBoolean closed = new AtomicBoolean();
        assert assertAddTranslogRef(closed);
        return () -> {
            if (closed.compareAndSet(false, true)) {
                releaseTranslogGen(translogGen);
                assert assertRemoveTranslogRef(closed);
            }
        };
    }

    private boolean assertAddTranslogRef(Object reference) {
        final RuntimeException existing = openTranslogRef.put(reference, new RuntimeException());
        if (existing != null) {
            throw new AssertionError("double adding of closing reference", existing);
        }
        return true;
    }

    private boolean assertRemoveTranslogRef(Object reference) {
        return openTranslogRef.remove(reference) != null;
    }

    /** returns the number of generations that were acquired for snapshots */
    synchronized int pendingTranslogRefCount() {
        return translogRefCounts.size();
    }

    /**
     * releases a generation that was acquired by {@link #acquireTranslogGen(long)}
     */
    private synchronized void releaseTranslogGen(long translogGen) {
        Counter current = translogRefCounts.get(translogGen);
        if (current == null || current.get() <= 0) {
            throw new IllegalArgumentException("translog gen [" + translogGen + "] wasn't acquired");
        }
        if (current.addAndGet(-1) == 0) {
            translogRefCounts.remove(translogGen);
        }
    }

    /**
     * Returns the minimum translog generation that is still required by the locks (via {@link #acquireTranslogGen(long)}.
     */
    synchronized long getMinTranslogGenRequiredByLocks() {
        return translogRefCounts.keySet().stream().reduce(Math::min).orElse(Long.MAX_VALUE);
    }

    /**
     * Returns the local checkpoint of the safe commit. This value is used to calculate the min required generation for recovery.
     */
    public synchronized long getLocalCheckpointOfSafeCommit() {
        return localCheckpointOfSafeCommit;
    }


    synchronized long getTranslogRefCount(long gen) {
        final Counter counter = translogRefCounts.get(gen);
        return counter == null ? 0 : counter.get();
    }
}
