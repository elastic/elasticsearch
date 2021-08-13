/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A very simple single object cache that allows non-blocking refresh calls
 * triggered by expiry time.
 */
public abstract class SingleObjectCache<T>{

    private volatile T cached;
    private final Lock refreshLock = new ReentrantLock();
    private final TimeValue refreshInterval;
    protected long lastRefreshTimestamp = 0;

    protected SingleObjectCache(TimeValue refreshInterval, T initialValue) {
        if (initialValue == null) {
            throw new IllegalArgumentException("initialValue must not be null");
        }
        this.refreshInterval = refreshInterval;
        cached = initialValue;
    }


    /**
     * Returns the currently cached object and potentially refreshes the cache before returning.
     */
    public T getOrRefresh() {
        if (needsRefresh()) {
            if(refreshLock.tryLock()) {
                try {
                    if (needsRefresh()) { // check again!
                        cached = refresh();
                        assert cached != null;
                        lastRefreshTimestamp = System.currentTimeMillis();
                    }
                } finally {
                    refreshLock.unlock();
                }
            }
        }
        assert cached != null;
        return cached;
    }

    /** Return the potentially stale cached entry. */
    protected final T getNoRefresh() {
        return cached;
    }

    /**
     * Returns a new instance to cache
     */
    protected abstract T refresh();

    /**
     * Returns <code>true</code> iff the cache needs to be refreshed.
     */
    protected boolean needsRefresh() {
        if (refreshInterval.millis() == 0) {
            return true;
        }
        final long currentTime = System.currentTimeMillis();
        return (currentTime - lastRefreshTimestamp) > refreshInterval.millis();
    }
}
