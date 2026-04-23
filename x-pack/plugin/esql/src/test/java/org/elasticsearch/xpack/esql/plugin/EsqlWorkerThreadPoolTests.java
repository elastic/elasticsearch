/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
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
}
