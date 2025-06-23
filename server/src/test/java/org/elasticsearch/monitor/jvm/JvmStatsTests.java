/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class JvmStatsTests extends ESTestCase {
    public void testJvmStats() {
        JvmStats stats = JvmStats.jvmStats();
        assertNotNull(stats);
        assertNotNull(stats.getUptime());
        assertThat(stats.getUptime().millis(), greaterThan(0L));
        assertThat(stats.getTimestamp(), greaterThan(0L));

        // Mem
        JvmStats.Mem mem = stats.getMem();
        assertNotNull(mem);
        for (ByteSizeValue heap : Arrays.asList(mem.getHeapCommitted(), mem.getHeapMax(), mem.getHeapUsed(), mem.getNonHeapCommitted())) {
            assertNotNull(heap);
            assertThat(heap.getBytes(), greaterThanOrEqualTo(0L));
        }
        assertNotNull(mem.getHeapUsedPercent());
        assertThat(mem.getHeapUsedPercent(), anyOf(equalTo((short) -1), greaterThanOrEqualTo((short) 0)));

        // Memory pools
        Map<String, JvmStats.MemoryPool> memoryPools = StreamSupport.stream(stats.getMem().spliterator(), false)
            .collect(Collectors.toMap(JvmStats.MemoryPool::getName, Function.identity()));
        assertThat(memoryPools, hasKey(GcNames.YOUNG));
        assertThat(memoryPools, hasKey(GcNames.OLD));
        assertThat(memoryPools, hasKey("Metaspace"));
        assertThat(memoryPools.keySet(), hasSize(greaterThan(3)));
        for (JvmStats.MemoryPool memoryPool : memoryPools.values()) {
            assertThat("Memory pool: " + memoryPool.getName(), memoryPool.getUsed().getBytes(), greaterThanOrEqualTo(0L));
        }

        // Threads
        JvmStats.Threads threads = stats.getThreads();
        assertNotNull(threads);
        assertThat(threads.getCount(), greaterThanOrEqualTo(0));
        assertThat(threads.getPeakCount(), greaterThanOrEqualTo(0));

        // GC
        JvmStats.GarbageCollectors gcs = stats.getGc();
        assertNotNull(gcs);

        JvmStats.GarbageCollector[] collectors = gcs.getCollectors();
        assertNotNull(collectors);
        assertThat(collectors.length, greaterThan(0));
        for (JvmStats.GarbageCollector collector : collectors) {
            assertTrue(Strings.hasText(collector.getName()));
            assertNotNull(collector.getCollectionTime());
            assertThat(collector.getCollectionTime().millis(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            assertThat(collector.getCollectionCount(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        }

        // Buffer Pools
        List<JvmStats.BufferPool> bufferPools = stats.getBufferPools();
        if (bufferPools != null) {
            for (JvmStats.BufferPool bufferPool : bufferPools) {
                assertNotNull(bufferPool);
                assertTrue(Strings.hasText(bufferPool.getName()));
                assertThat(bufferPool.getCount(), greaterThanOrEqualTo(0L));
                assertNotNull(bufferPool.getTotalCapacity());
                assertThat(bufferPool.getTotalCapacity().getBytes(), greaterThanOrEqualTo(0L));
                assertNotNull(bufferPool.getUsed());
                assertThat(bufferPool.getUsed().getBytes(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
            }
        }

        // Classes
        JvmStats.Classes classes = stats.getClasses();
        assertNotNull(classes);
        assertThat(classes.getLoadedClassCount(), greaterThanOrEqualTo(0L));
        assertThat(classes.getTotalLoadedClassCount(), greaterThanOrEqualTo(0L));
        assertThat(classes.getUnloadedClassCount(), greaterThanOrEqualTo(0L));
    }
}
