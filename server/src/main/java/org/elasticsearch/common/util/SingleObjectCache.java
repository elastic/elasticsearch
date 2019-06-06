/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A very simple single object cache that allows non-blocking refresh calls
 * triggered by expiry time.
 */
public abstract class SingleObjectCache<T>{

    private volatile T cached;
    private Lock refreshLock = new ReentrantLock();
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
