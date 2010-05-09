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

import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.inject.Inject;

/**
 * @author kimchy (shay.banon)
 */
@MBean(objectName = "service=threadpool,threadpoolType=blocking", description = "Blocking Thread Pool")
public class BlockingThreadPoolManagement {

    private final BlockingThreadPool threadPool;

    @Inject public BlockingThreadPoolManagement(ThreadPool threadPool) {
        this.threadPool = (BlockingThreadPool) threadPool;
    }

    @ManagedAttribute(description = "Minimum number Of threads")
    public long getMin() {
        return threadPool.min;
    }

    @ManagedAttribute(description = "Maximum number of threads")
    public int getMax() {
        return threadPool.max;
    }

    @ManagedAttribute(description = "Number of scheduler threads")
    public int getScheduleSize() {
        return threadPool.scheduledSize;
    }

    @ManagedAttribute(description = "Thread keep alive")
    public String getKeepAlive() {
        return threadPool.keepAlive.format();
    }

    @ManagedAttribute(description = "Thread keep alive (in seconds)")
    public long getKeepAliveInSeconds() {
        return threadPool.keepAlive.seconds();
    }

    @ManagedAttribute(description = "Current number of threads in the pool")
    public long getPoolSize() {
        return threadPool.getPoolSize();
    }

    @ManagedAttribute(description = "Approximate number of threads that are actively executing tasks")
    public long getActiveCount() {
        return threadPool.getActiveCount();
    }

    @ManagedAttribute(description = "Current number of threads in the scheduler pool")
    public long getSchedulerPoolSize() {
        return threadPool.getSchedulerPoolSize();
    }

    @ManagedAttribute(description = "Approximate number of threads that are actively executing scheduled tasks")
    public long getSchedulerActiveCount() {
        return threadPool.getSchedulerActiveCount();
    }
}