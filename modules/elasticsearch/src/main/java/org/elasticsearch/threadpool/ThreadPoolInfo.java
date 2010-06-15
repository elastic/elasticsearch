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
import org.elasticsearch.common.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * Thread Pool Info.
 *
 * @author kimchy (shay.banon)
 */
public class ThreadPoolInfo implements Streamable, Serializable, ToXContent {

    private String type;

    private int minThreads;

    private int maxThreads;

    private int schedulerThreads;

    ThreadPoolInfo() {
    }

    public ThreadPoolInfo(String type, int minThreads, int maxThreads, int schedulerThreads) {
        this.type = type;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.schedulerThreads = schedulerThreads;
    }

    public static ThreadPoolInfo readThreadPoolInfo(StreamInput in) throws IOException {
        ThreadPoolInfo info = new ThreadPoolInfo();
        info.readFrom(in);
        return info;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        type = in.readUTF();
        minThreads = in.readInt();
        maxThreads = in.readInt();
        schedulerThreads = in.readInt();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(type);
        out.writeInt(minThreads);
        out.writeInt(maxThreads);
        out.writeInt(schedulerThreads);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("thread_pool");
        builder.field("type", type);
        builder.field("min_threads", minThreads);
        builder.field("max_threads", maxThreads);
        builder.field("scheduler_threads", schedulerThreads);
        builder.endObject();
    }

    /**
     * The type of the thread pool.
     */
    public String type() {
        return type;
    }

    /**
     * The type of the thread pool.
     */
    public String getType() {
        return type();
    }

    /**
     * The minimum number of threads in the thread pool.
     */
    public int minThreads() {
        return minThreads;
    }

    /**
     * The minimum number of threads in the thread pool.
     */
    public int getMinThreads() {
        return minThreads();
    }

    /**
     * The maximum number of threads in the thread pool.
     */
    public int maxThreads() {
        return maxThreads;
    }

    /**
     * The maximum number of threads in the thread pool.
     */
    public int getMaxThreads() {
        return maxThreads();
    }

    /**
     * The size of scheduler threads.
     */
    public int schedulerThreads() {
        return schedulerThreads;
    }

    /**
     * The size of scheduler threads.
     */
    public int getSchedulerThreads() {
        return schedulerThreads();
    }
}
