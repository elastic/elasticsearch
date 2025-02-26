/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

class EngineReferenceManager {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true); // fair
    private final Releasable releaseExclusiveLock = () -> lock.writeLock().unlock();  // reuse this to avoid allocation for each op
    private final Releasable releaseLock = () -> lock.readLock().unlock();            // reuse this to avoid allocation for each op

    private volatile Engine current;

    /**
     * @return a releasable reference to a given {@link Engine}, preventing it to be changed until the reference is released. Note that the
     * {@link Engine} referenced by the {@link EngineRef} may be null and may be closed at anytime in case of an engine failure, but is
     * guaranteed to not be changed (and closed) by an engine reset.
     */
    public EngineRef getEngineRef() {
        lock.readLock().lock();
        return new EngineRef(this.current, Releasables.assertOnce(releaseLock));
    }

    /**
     * Acquires a lock that prevents the {@link Engine} to be changed until the returned releasable is released.
     * @return
     */
    Releasable acquireEngineLock() {
        lock.writeLock().lock();
        return Releasables.assertOnce(releaseExclusiveLock);
    }

    boolean isEngineLockHeldByCurrentThread() {
        return lock.writeLock().isHeldByCurrentThread();
    }

    /**
     * @return the (possibly null) current reference to the {@link Engine}
     */
    @Nullable
    Engine get() {
        return this.current;
    }

    @Nullable
    Engine getEngineAndSet(Supplier<Engine> supplier) {
        assert supplier != null : "supplier cannot be null";
        assert isEngineLockHeldByCurrentThread();
        Engine previous = this.current;
        this.current = supplier.get();
        return previous;
    }
}
