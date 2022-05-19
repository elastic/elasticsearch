/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.threadpool.ThreadPool;

/**
 * A class that gathers the names of thread pool executors that should be used for a particular system index or system data stream. This
 * object is used both by the {@link SystemIndexDescriptor} and the {@link SystemDataStreamDescriptor} classes.
 */
public class ExecutorNames {
    private final String getPoolName;
    private final String searchPoolName;
    private final String writePoolName;

    /**
     * The thread pools for a typical system index.
     */
    public static ExecutorNames DEFAULT_SYSTEM_INDEX_THREAD_POOLS = new ExecutorNames(
        ThreadPool.Names.SYSTEM_READ,
        ThreadPool.Names.SYSTEM_READ,
        ThreadPool.Names.SYSTEM_WRITE
    );

    /**
     * The thread pools for a typical system data stream. These are also the usual thread pools for non-system indices and data streams.
     */
    public static ExecutorNames DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS = new ExecutorNames(
        ThreadPool.Names.GET,
        ThreadPool.Names.SEARCH,
        ThreadPool.Names.WRITE
    );

    /**
     * The thread pools that should be used for critical system index operations.
     */
    public static ExecutorNames CRITICAL_SYSTEM_INDEX_THREAD_POOLS = new ExecutorNames(
        ThreadPool.Names.SYSTEM_CRITICAL_READ,
        ThreadPool.Names.SYSTEM_CRITICAL_READ,
        ThreadPool.Names.SYSTEM_CRITICAL_WRITE
    );

    /**
     * Create a new collection of thread pool names for a system descriptor to use.
     *
     * @param getPoolName    Name of the thread pool that get operations should use.
     * @param searchPoolName Name of the thread pool that search operations should use. (In same cases, this is the same as the name of the
     *                       pool for get operations.)
     * @param writePoolName  Name of the thread pool that write operations should use.
     */
    public ExecutorNames(String getPoolName, String searchPoolName, String writePoolName) {
        if (ThreadPool.THREAD_POOL_TYPES.containsKey(getPoolName) == false) {
            throw new IllegalArgumentException(getPoolName + " is not a valid thread pool");
        }
        if (ThreadPool.THREAD_POOL_TYPES.containsKey(searchPoolName) == false) {
            throw new IllegalArgumentException(searchPoolName + " is not a valid thread pool");
        }
        if (ThreadPool.THREAD_POOL_TYPES.containsKey(writePoolName) == false) {
            throw new IllegalArgumentException(writePoolName + " is not a valid thread pool");
        }
        this.getPoolName = getPoolName;
        this.searchPoolName = searchPoolName;
        this.writePoolName = writePoolName;
    }

    /**
     * @return Name of the thread pool that get operations should use
     */
    public String threadPoolForGet() {
        return getPoolName;
    }

    /**
     * @return Name of the thread pool that search operations should use
     */
    public String threadPoolForSearch() {
        return searchPoolName;
    }

    /**
     * @return Name of the thread pool that write operations should use
     */
    public String threadPoolForWrite() {
        return writePoolName;
    }
}
