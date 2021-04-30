/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExecutorSelectorServiceTests extends ESTestCase {

    public void testNonCriticalSystemIndexThreadPools() {
        ExecutorSelectorService service = new ExecutorSelectorService(new SystemIndices(
            Map.of(
                "normal system index",
                new SystemIndices.Feature( "normal", "normal system index",
                    Collections.singletonList(new SystemIndexDescriptor( ".non-critical-system-index", "test index"))
                )
            )
        ));
        String index = ".non-critical-system-index";
        assertThat(service.getGetExecutor(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.getReadExecutor(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.getWriteExecutor(index), equalTo(ThreadPool.Names.SYSTEM_WRITE));
    }

    public void testCriticalSystemIndexThreadPools() {
        ExecutorSelectorService service = new ExecutorSelectorService(new SystemIndices(
            Map.of(
                "critical system index",
                new SystemIndices.Feature( "critical", "critical system index", Collections.singletonList(
                    SystemIndexDescriptor.builder()
                        .setDescription("critical system indices")
                        .setIndexPattern(".critical-system-*")
                        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                        .setThreadPools(SystemIndices.ThreadPools.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
                        .build()
                ))
            )
        ));
        String index = ".critical-system-index";
        assertThat(service.getGetExecutor(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.getReadExecutor(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.getWriteExecutor(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_WRITE));
    }

    public void testDefaultSystemDataStreamThreadPools() {
        ExecutorSelectorService service = new ExecutorSelectorService(new SystemIndices(
            Map.of(
            "normal system index",
                new SystemIndices.Feature( "data stream", "data stream feature with default thread pools", Collections.emptyList(),
                    Collections.singletonList(
                        new SystemDataStreamDescriptor( ".test-data-stream", "a data stream for testing",
                            SystemDataStreamDescriptor.Type.INTERNAL,
                            new ComposableIndexTemplate(
                                List.of(".system-data-stream"),
                                null, null, null, null, null,
                                new ComposableIndexTemplate.DataStreamTemplate()),
                            Map.of(),
                            Collections.singletonList("test"),
                            null
                        )
                    )
                )
            )
        ));
        String dataStream = ".test-data-stream";
        assertThat(service.getGetExecutor(dataStream), equalTo(ThreadPool.Names.GET));
        assertThat(service.getReadExecutor(dataStream), equalTo(ThreadPool.Names.SEARCH));
        assertThat(service.getWriteExecutor(dataStream), equalTo(ThreadPool.Names.WRITE));
    }

    public void testCustomSystemDataStreamThreadPools() {
        ExecutorSelectorService service = new ExecutorSelectorService(new SystemIndices(
            Map.of(
                "normal system index",
                new SystemIndices.Feature( "data stream", "data stream feature with custom thread pools", Collections.emptyList(),
                    Collections.singletonList(
                        new SystemDataStreamDescriptor( ".test-data-stream", "a data stream for testing",
                            SystemDataStreamDescriptor.Type.INTERNAL,
                            new ComposableIndexTemplate(
                                List.of(".system-data-stream"),
                                null, null, null, null, null,
                                new ComposableIndexTemplate.DataStreamTemplate()),
                            Map.of(),
                            Collections.singletonList("test"),
                            new SystemIndices.ThreadPools(
                                ThreadPool.Names.SYSTEM_CRITICAL_READ, ThreadPool.Names.SYSTEM_READ, ThreadPool.Names.SYSTEM_WRITE)
                        )
                    )
                )
            )
        ));
        String dataStream = ".test-data-stream";
        assertThat(service.getGetExecutor(dataStream), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.getReadExecutor(dataStream), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.getWriteExecutor(dataStream), equalTo(ThreadPool.Names.SYSTEM_WRITE));
    }

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
