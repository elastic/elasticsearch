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

public class ExecutorSelectorTests extends ESTestCase {

    public void testNonCriticalSystemIndexThreadPools() {
        ExecutorSelector service = new ExecutorSelector(
            new SystemIndices(
                List.of(
                    new SystemIndices.Feature(
                        "normal",
                        "normal system index",
                        Collections.singletonList(SystemIndexDescriptorUtils.createUnmanaged(".non-critical-system-index*", "test index"))
                    )
                )
            )
        );
        String index = ".non-critical-system-index";
        assertThat(service.executorForGet(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.executorForSearch(index), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.executorForWrite(index), equalTo(ThreadPool.Names.SYSTEM_WRITE));
    }

    public void testCriticalSystemIndexThreadPools() {
        ExecutorSelector service = new ExecutorSelector(
            new SystemIndices(
                List.of(
                    new SystemIndices.Feature(
                        "critical",
                        "critical system index",
                        Collections.singletonList(
                            SystemIndexDescriptor.builder()
                                .setDescription("critical system indices")
                                .setIndexPattern(".critical-system-*")
                                .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                                .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
                                .build()
                        )
                    )
                )
            )
        );
        String index = ".critical-system-index";
        assertThat(service.executorForGet(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.executorForSearch(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.executorForWrite(index), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_WRITE));
    }

    public void testDefaultSystemDataStreamThreadPools() {
        ExecutorSelector service = new ExecutorSelector(
            new SystemIndices(
                List.of(
                    new SystemIndices.Feature(
                        "data stream",
                        "data stream feature with default thread pools",
                        Collections.emptyList(),
                        Collections.singletonList(
                            new SystemDataStreamDescriptor(
                                ".test-data-stream",
                                "a data stream for testing",
                                SystemDataStreamDescriptor.Type.INTERNAL,
                                new ComposableIndexTemplate(
                                    List.of(".system-data-stream"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new ComposableIndexTemplate.DataStreamTemplate()
                                ),
                                Map.of(),
                                Collections.singletonList("test"),
                                null
                            )
                        )
                    )
                )
            )
        );
        String dataStream = ".test-data-stream";
        assertThat(service.executorForGet(dataStream), equalTo(ThreadPool.Names.GET));
        assertThat(service.executorForSearch(dataStream), equalTo(ThreadPool.Names.SEARCH));
        assertThat(service.executorForWrite(dataStream), equalTo(ThreadPool.Names.WRITE));
    }

    public void testCustomSystemDataStreamThreadPools() {
        ExecutorSelector service = new ExecutorSelector(
            new SystemIndices(
                List.of(
                    new SystemIndices.Feature(
                        "data stream",
                        "data stream feature with custom thread pools",
                        Collections.emptyList(),
                        Collections.singletonList(
                            new SystemDataStreamDescriptor(
                                ".test-data-stream",
                                "a data stream for testing",
                                SystemDataStreamDescriptor.Type.INTERNAL,
                                new ComposableIndexTemplate(
                                    List.of(".system-data-stream"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new ComposableIndexTemplate.DataStreamTemplate()
                                ),
                                Map.of(),
                                Collections.singletonList("test"),
                                new ExecutorNames(
                                    ThreadPool.Names.SYSTEM_CRITICAL_READ,
                                    ThreadPool.Names.SYSTEM_READ,
                                    ThreadPool.Names.SYSTEM_WRITE
                                )
                            )
                        )
                    )
                )
            )
        );
        String dataStream = ".test-data-stream";
        assertThat(service.executorForGet(dataStream), equalTo(ThreadPool.Names.SYSTEM_CRITICAL_READ));
        assertThat(service.executorForSearch(dataStream), equalTo(ThreadPool.Names.SYSTEM_READ));
        assertThat(service.executorForWrite(dataStream), equalTo(ThreadPool.Names.SYSTEM_WRITE));
    }

    public void testCreateThreadPools() {
        String getThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());
        String searchThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());
        String writeThreadPool = randomFrom(ThreadPool.THREAD_POOL_TYPES.keySet());

        ExecutorNames executorNames = new ExecutorNames(getThreadPool, searchThreadPool, writeThreadPool);

        assertThat(executorNames.threadPoolForGet(), equalTo(getThreadPool));
        assertThat(executorNames.threadPoolForSearch(), equalTo(searchThreadPool));
        assertThat(executorNames.threadPoolForWrite(), equalTo(writeThreadPool));
    }

    public void testInvalidThreadPoolNames() {
        String invalidThreadPool = randomValueOtherThanMany(ThreadPool.THREAD_POOL_TYPES::containsKey, () -> randomAlphaOfLength(8));

        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new ExecutorNames(invalidThreadPool, ThreadPool.Names.SEARCH, ThreadPool.Names.WRITE)
            );

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new ExecutorNames(ThreadPool.Names.GET, invalidThreadPool, ThreadPool.Names.WRITE)
            );

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new ExecutorNames(ThreadPool.Names.GET, ThreadPool.Names.SEARCH, invalidThreadPool)
            );

            assertThat(exception.getMessage(), containsString(invalidThreadPool + " is not a valid thread pool"));
        }
    }
}
