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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * Thread Pool level stats.
 *
 * @author kimchy (shay.banon)
 */
public class ThreadPoolStats implements Streamable, Serializable, ToXContent {

    private int poolSize;

    private int activeCount;

    private int schedulerPoolSize;

    private int schedulerActiveCount;

    ThreadPoolStats() {
    }

    public ThreadPoolStats(int poolSize, int activeCount, int schedulerPoolSize, int schedulerActiveCount) {
        this.poolSize = poolSize;
        this.activeCount = activeCount;
        this.schedulerPoolSize = schedulerPoolSize;
        this.schedulerActiveCount = schedulerActiveCount;
    }

    public static ThreadPoolStats readThreadPoolStats(StreamInput in) throws IOException {
        ThreadPoolStats stats = new ThreadPoolStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        poolSize = in.readVInt();
        activeCount = in.readVInt();
        schedulerPoolSize = in.readVInt();
        schedulerActiveCount = in.readVInt();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(poolSize);
        out.writeVInt(activeCount);
        out.writeVInt(schedulerPoolSize);
        out.writeVInt(schedulerActiveCount);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("thread_pool");
        builder.field("pool_size", poolSize);
        builder.field("active_count", activeCount);
        builder.field("scheduler_pool_size", schedulerPoolSize);
        builder.field("scheduler_active_count", schedulerActiveCount);
        builder.endObject();
    }

    /**
     * Returns the current number of threads in the pool.
     *
     * @return the number of threads
     */
    public int poolSize() {
        return poolSize;
    }

    /**
     * Returns the current number of threads in the pool.
     *
     * @return the number of threads
     */
    public int getPoolSize() {
        return poolSize();
    }

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     */
    public int activeCount() {
        return activeCount;
    }

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     */
    public int getActiveCount() {
        return activeCount();
    }

    /**
     * The size of the scheduler thread pool.
     */
    public int schedulerPoolSize() {
        return schedulerPoolSize;
    }

    /**
     * The size of the scheduler thread pool.
     */
    public int getSchedulerPoolSize() {
        return schedulerPoolSize();
    }

    /**
     * The approximate number of threads that are actively executing scheduled
     * tasks.
     */
    public int schedulerActiveCount() {
        return schedulerActiveCount;
    }

    /**
     * The approximate number of threads that are actively executing scheduled
     * tasks.
     */
    public int getSchedulerActiveCount() {
        return schedulerActiveCount();
    }
}
