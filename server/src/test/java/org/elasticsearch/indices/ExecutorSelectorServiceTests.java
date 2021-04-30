/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExecutorSelectorServiceTests extends ESTestCase {

    private static SystemIndices SYSTEM_INDICES = new SystemIndices(
        Map.of(
            "normal system index", new SystemIndices.Feature("hi", "there", List.of(
                new SystemIndexDescriptor(".non-critical-system-index", "test index")
            ))
        ));

    public void testNonCriticalSystemIndexThreadPools() {
        ExecutorSelectorService service = new ExecutorSelectorService(SYSTEM_INDICES);
        String index = ".non-critical-system-index";
        assertThat(service.getGetExecutor(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.getReadExecutor(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.getWriteExecutor(index), equalTo(ThreadPool.Names.SYSTEM_WRITE));
    }

    // TODO: critical thread pools

    // TODO: data streams

    public void testCreateThreadPools() {
        String getThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());
        String searchThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());
        String writeThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());

        SystemIndices.ThreadPools threadPools = new SystemIndices.ThreadPools(getThreadPool, searchThreadPool, writeThreadPool);

        assertThat(threadPools.getGetPoolName(), equalTo(getThreadPool));
        assertThat(threadPools.getSearchPoolName(), equalTo(searchThreadPool));
        assertThat(threadPools.getWritePoolName(), equalTo(writeThreadPool));
    }

    public void testInvalidThreadPoolNames() {
        String invalidThreadPool = randomValueOtherThanMany(
            ThreadPool.THREAD_POOL_TYPES::containsKey,
            () -> randomAlphaOfLength(8));

        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndices.ThreadPools(invalidThreadPool, ThreadPool.Names.SEARCH, ThreadPool.Names.WRITE));

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndices.ThreadPools(ThreadPool.Names.GET, invalidThreadPool, ThreadPool.Names.WRITE));

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndices.ThreadPools(ThreadPool.Names.GET, ThreadPool.Names.SEARCH, invalidThreadPool));

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
    }
}
