/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.logstashbridge.DucktypeMatchers.instancesQuackLike;
import static org.elasticsearch.logstashbridge.DucktypeMatchers.staticallyQuacksLike;

public class ThreadPoolBridgeTests extends ESTestCase {

    public void testThreadPoolAPIStability() throws Exception {
        interface ThreadPoolInstanceAPI {
            long relativeTimeInMillis();

            long absoluteTimeInMillis();

            ThreadContext getThreadContext();

            Scheduler.ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor);

            ExecutorService generic();
        }
        assertThat(ThreadPool.class, instancesQuackLike(ThreadPoolInstanceAPI.class));

        interface ThreadPoolStaticAPI {
            boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit);
        }
        assertThat(ThreadPool.class, staticallyQuacksLike(ThreadPoolStaticAPI.class));
    }
}
