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

package org.elasticsearch.threadpool.blocking;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.support.AbstractThreadPool;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.concurrent.DynamicExecutors;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * A thread pool that will will block the execute if all threads are busy.
 *
 * @author kimchy (shay.banon)
 */
public class BlockingThreadPool extends AbstractThreadPool {

    final int min;
    final int max;
    final int capacity;
    final TimeValue waitTime;
    final TimeValue keepAlive;

    final int scheduledSize;

    public BlockingThreadPool() {
        this(EMPTY_SETTINGS);
    }

    @Inject public BlockingThreadPool(Settings settings) {
        super(settings);
        this.scheduledSize = componentSettings.getAsInt("scheduled_size", 20);
        this.min = componentSettings.getAsInt("min", 1);
        this.max = componentSettings.getAsInt("max", 100);
        this.capacity = (int) componentSettings.getAsSize("capacity", new SizeValue(1, SizeUnit.KB)).bytes();
        this.waitTime = componentSettings.getAsTime("wait_time", timeValueSeconds(60));
        this.keepAlive = componentSettings.getAsTime("keep_alive", timeValueSeconds(60));
        logger.debug("Initializing {} thread pool with min[{}], max[{}], keep_alive[{}], capacity[{}], wait_time[{}], scheduled_size[{}]", getType(), min, max, keepAlive, capacity, waitTime, scheduledSize);
        executorService = DynamicExecutors.newBlockingThreadPool(min, max, keepAlive.millis(), capacity, waitTime.millis(), DynamicExecutors.daemonThreadFactory(settings, "[tp]"));
        scheduledExecutorService = Executors.newScheduledThreadPool(scheduledSize, DynamicExecutors.daemonThreadFactory(settings, "[sc]"));
        started = true;
    }

    @Override public String getType() {
        return "blocking";
    }

    @Override public int getMinThreads() {
        return min;
    }

    @Override public int getMaxThreads() {
        return max;
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