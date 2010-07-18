/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.threadpool.cached;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.support.AbstractThreadPool;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * A thread pool that will create an unbounded number of threads.
 *
 * @author kimchy (shay.banon)
 */
public class CachedThreadPool extends AbstractThreadPool {

    final TimeValue keepAlive;

    final int scheduledSize;

    public CachedThreadPool() {
        this(EMPTY_SETTINGS);
    }

    @Inject public CachedThreadPool(Settings settings) {
        super(settings);
        this.scheduledSize = componentSettings.getAsInt("scheduled_size", 20);
        this.keepAlive = componentSettings.getAsTime("keep_alive", timeValueSeconds(60));
        logger.debug("Initializing {} thread pool with keep_alive[{}], scheduled_size[{}]", getType(), keepAlive, scheduledSize);
        executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                keepAlive.millis(), TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                EsExecutors.daemonThreadFactory(settings, "[tp]"));
        scheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(scheduledSize, EsExecutors.daemonThreadFactory(settings, "[sc]"));
        cached = executorService;
        started = true;
    }

    @Override public String getType() {
        return "cached";
    }

    @Override public int getMinThreads() {
        return 0;
    }

    @Override public int getMaxThreads() {
        return -1;
    }

    @Override public int getSchedulerThreads() {
        return scheduledSize;
    }

    @Override public int getPoolSize() {
        return ((ThreadPoolExecutor) executorService).getPoolSize();
    }

    @Override public int getActiveCount() {
        return ((ThreadPoolExecutor) executorService).getActiveCount();
    }

    @Override public int getSchedulerPoolSize() {
        return ((ThreadPoolExecutor) scheduledExecutorService).getPoolSize();
    }

    @Override public int getSchedulerActiveCount() {
        return ((ThreadPoolExecutor) scheduledExecutorService).getActiveCount();
    }
}
