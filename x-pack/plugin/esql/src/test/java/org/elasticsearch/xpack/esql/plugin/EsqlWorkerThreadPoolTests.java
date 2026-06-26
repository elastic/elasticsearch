/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;

import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_SIZE;

public class EsqlWorkerThreadPoolTests extends ESTestCase {

    public void testDefaultThreadPoolSize() {
        EsqlPlugin plugin = new EsqlPlugin();
        Settings settings = Settings.EMPTY;
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(settings);
        assertEquals(1, builders.size());
    }

    public void testCustomThreadPoolSize() {
        Settings settings = Settings.builder().put(ESQL_WORKER_THREAD_POOL_SIZE.getKey(), 42).build();
        int configured = ESQL_WORKER_THREAD_POOL_SIZE.get(settings);
        assertEquals(42, configured);
    }

    public void testDefaultSettingValue() {
        Settings settings = Settings.EMPTY;
        int configured = ESQL_WORKER_THREAD_POOL_SIZE.get(settings);
        assertEquals(-1, configured);
    }

    public void testSettingRegistered() {
        EsqlPlugin plugin = new EsqlPlugin();
        boolean found = false;
        for (var setting : plugin.getSettings()) {
            if (setting.getKey().equals(ESQL_WORKER_THREAD_POOL_SIZE.getKey())) {
                found = true;
                break;
            }
        }
        assertTrue("ESQL_WORKER_THREAD_POOL_SIZE should be registered", found);
    }

    public void testThreadPoolNameConstant() {
        assertEquals("esql_worker", ESQL_WORKER_THREAD_POOL_NAME);
    }

    // --- worker queue size formula ---

    public void testWorkerQueueSizeFloor() {
        // with no heap and a single CPU the floor of 1000 must hold
        assertEquals(1000, EsqlPlugin.workerQueueSize(0, 1));
    }

    public void testWorkerQueueSizeFloorWithSmallValues() {
        // 512 MB heap, 4 CPUs: (512 + 4*400)/2 = (512+1600)/2 = 1056, above floor
        assertEquals(1056, EsqlPlugin.workerQueueSize(512L * 1024 * 1024, 4));
    }

    public void testWorkerQueueSizeScalesWithHeap() {
        // 4 GB heap, 8 CPUs: (4096 + 8*400)/2 = (4096+3200)/2 = 3648
        assertEquals(3648, EsqlPlugin.workerQueueSize(4L * 1024 * 1024 * 1024, 8));
    }

    public void testWorkerQueueSizeScalesWithCpus() {
        // 0 heap, enough CPUs to exceed 1000: need (n*400)/2 > 1000 => n > 5
        // 6 CPUs: (0 + 6*400)/2 = 1200 > 1000
        assertEquals(1200, EsqlPlugin.workerQueueSize(0, 6));
    }

    public void testWorkerQueueSizeNeverBelowFloor() {
        // exhaustive check: any combination of tiny heap and few CPUs must stay >= 1000
        for (int cpus = 1; cpus <= 5; cpus++) {
            for (long heapMb = 0; heapMb <= 1000; heapMb += 100) {
                int size = EsqlPlugin.workerQueueSize(heapMb * 1024 * 1024, cpus);
                assertTrue("queue size must be >= 1000, got " + size + " for " + heapMb + "MB heap, " + cpus + " CPUs", size >= 1000);
            }
        }
    }

    public void testGetExecutorBuildersUsesFormula() {
        // verify the builder registered for the esql_worker pool defaults to the formula result
        EsqlPlugin plugin = new EsqlPlugin();
        Settings settings = Settings.EMPTY;
        int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        int expectedQueueSize = EsqlPlugin.workerQueueSize(heapBytes, allocatedProcessors);

        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(settings);
        ExecutorBuilder<?> workerBuilder = builders.get(0);
        Setting<?> queueSizeSetting = workerBuilder.getRegisteredSettings()
            .stream()
            .filter(s -> s.getKey().equals(ESQL_WORKER_THREAD_POOL_NAME + ".queue_size"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("queue_size setting not registered"));

        assertEquals(expectedQueueSize, queueSizeSetting.getDefault(Settings.EMPTY));
    }
}
