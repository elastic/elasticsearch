/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Releasable;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A driver-local context that is shared across operators.
 *
 * Operators in the same driver pipeline are executed in a single threaded fashion. A driver context
 * has a set of mutating methods that can be used to store and share values across these operators,
 * or even outside the Driver. When the Driver is finished, it finishes the context. Finishing the
 * context effectively takes a snapshot of the driver context values so that they can be exposed
 * outside the Driver. The net result of this is that the driver context can be mutated freely,
 * without contention, by the thread executing the pipeline of operators until it is finished.
 * The context must be finished by the thread running the Driver, when the Driver is finished.
 *
 * Releasables can be added and removed to the context by operators in the same driver pipeline.
 * This allows to "transfer ownership" of a shared resource across operators (and even across
 * Drivers), while ensuring that the resource can be correctly released when no longer needed.
 *
 * Currently only supports releasables, but additional driver-local context can be added.
 */
public class DriverContext {

    // Working set. Only the thread executing the driver will update this set.
    Set<Releasable> workingSet = Collections.newSetFromMap(new IdentityHashMap<>());

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();

    /** A snapshot of the driver context. */
    public record Snapshot(Set<Releasable> releasables) {}

    /**
     * Adds a releasable to this context. Releasables are identified by Object identity.
     * @return true if the releasable was added, otherwise false (if already present)
     */
    public boolean addReleasable(Releasable releasable) {
        return workingSet.add(releasable);
    }

    /**
     * Removes a releasable from this context. Releasables are identified by Object identity.
     * @return true if the releasable was removed, otherwise false (if not present)
     */
    public boolean removeReleasable(Releasable releasable) {
        return workingSet.remove(releasable);
    }

    /**
     * Retrieves the snapshot of the driver context after it has been finished.
     * @return the snapshot
     */
    public Snapshot getSnapshot() {
        ensureFinished();
        // should be called by the DriverRunner
        return snapshot.get();
    }

    /**
     * Tells whether this context is finished. Can be invoked from any thread.
     */
    public boolean isFinished() {
        return snapshot.get() != null;
    }

    /**
     * Finishes this context. Further mutating operations should not be performed.
     */
    public void finish() {
        if (isFinished()) {
            return;
        }
        // must be called by the thread executing the driver.
        // no more updates to this context.
        var itr = workingSet.iterator();
        workingSet = null;
        Set<Releasable> releasableSet = Collections.newSetFromMap(new IdentityHashMap<>());
        while (itr.hasNext()) {
            var r = itr.next();
            releasableSet.add(r);
            itr.remove();
        }
        snapshot.compareAndSet(null, new Snapshot(releasableSet));
    }

    private void ensureFinished() {
        if (isFinished() == false) {
            throw new IllegalStateException("not finished");
        }
    }
}
