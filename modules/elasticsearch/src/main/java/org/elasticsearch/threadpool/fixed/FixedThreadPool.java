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

package org.elasticsearch.threadpool.fixed;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.ForkJoinPool;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.threadpool.support.AbstractThreadPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 *
 */
public class FixedThreadPool extends AbstractThreadPool {

    final int size;
    final TimeValue keepAlive;

    final int scheduledSize;

    public FixedThreadPool() {
        this(EMPTY_SETTINGS);
    }

    @Inject public FixedThreadPool(Settings settings) {
        super(settings);
        this.size = componentSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5);
        this.keepAlive = componentSettings.getAsTime("keep_alive", timeValueMinutes(5));
        this.scheduledSize = componentSettings.getAsInt("scheduled_size", 1);
        logger.debug("Initializing {} thread pool with [{}] threads, keep_alive[{}], scheduled_size[{}]", getType(), size, keepAlive, scheduledSize);
        scheduledExecutorService = Executors.newScheduledThreadPool(scheduledSize, EsExecutors.daemonThreadFactory(settings, "[sc]"));
        String type = componentSettings.get("type");
        if ("forkjoin".equalsIgnoreCase(type)) {
            executorService = new ForkJoinPool(size, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        } else {
            executorService = new ThreadPoolExecutor(size, size,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedTransferQueue<Runnable>(),
                    EsExecutors.daemonThreadFactory(settings, "[tp]"));
        }

        cached = EsExecutors.newCachedThreadPool(keepAlive, EsExecutors.daemonThreadFactory(settings, "[cached]"));
        started = true;
    }

    @Override public int getMinThreads() {
        return size;
    }

    @Override public int getMaxThreads() {
        return size;
    }

    @Override public int getSchedulerThreads() {
        return scheduledSize;
    }

    @Override public int getPoolSize() {
        if (executorService instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executorService).getPoolSize();
        }
        return -1;
    }

    @Override public int getActiveCount() {
        if (executorService instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executorService).getActiveCount();
        }
        return -1;
    }

    @Override public int getSchedulerPoolSize() {
        return ((ThreadPoolExecutor) scheduledExecutorService).getPoolSize();
    }

    @Override public int getSchedulerActiveCount() {
        return ((ThreadPoolExecutor) scheduledExecutorService).getActiveCount();
    }

    @Override public String getType() {
        return "fixed";
    }
}