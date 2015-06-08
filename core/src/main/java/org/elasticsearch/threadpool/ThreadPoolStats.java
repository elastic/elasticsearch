/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ThreadPoolStats implements Streamable, ToXContent, Iterable<ThreadPoolStats.Stats> {

    public static class Stats implements Streamable, ToXContent {

        private String name;
        private int threads;
        private int queue;
        private int active;
        private long rejected;
        private int largest;
        private long completed;

        Stats() {

        }

        public Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
        }

        public String getName() {
            return this.name;
        }

        public int getThreads() {
            return this.threads;
        }

        public int getQueue() {
            return this.queue;
        }

        public int getActive() {
            return this.active;
        }

        public long getRejected() {
            return rejected;
        }

        public int getLargest() {
            return largest;
        }

        public long getCompleted() {
            return this.completed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            threads = in.readInt();
            queue = in.readInt();
            active = in.readInt();
            rejected = in.readLong();
            largest = in.readInt();
            completed = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(threads);
            out.writeInt(queue);
            out.writeInt(active);
            out.writeLong(rejected);
            out.writeInt(largest);
            out.writeLong(completed);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name, XContentBuilder.FieldCaseConversion.NONE);
            if (threads != -1) {
                builder.field(Fields.THREADS, threads);
            }
            if (queue != -1) {
                builder.field(Fields.QUEUE, queue);
            }
            if (active != -1) {
                builder.field(Fields.ACTIVE, active);
            }
            if (rejected != -1) {
                builder.field(Fields.REJECTED, rejected);
            }
            if (largest != -1) {
                builder.field(Fields.LARGEST, largest);
            }
            if (completed != -1) {
                builder.field(Fields.COMPLETED, completed);
            }
            builder.endObject();
            return builder;
        }
    }

    private List<Stats> stats;

    ThreadPoolStats() {

    }

    public ThreadPoolStats(List<Stats> stats) {
        this.stats = stats;
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    public static ThreadPoolStats readThreadPoolStats(StreamInput in) throws IOException {
        ThreadPoolStats stats = new ThreadPoolStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        stats = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Stats stats1 = new Stats();
            stats1.readFrom(in);
            stats.add(stats1);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(stats.size());
        for (Stats stat : stats) {
            stat.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString THREAD_POOL = new XContentBuilderString("thread_pool");
        static final XContentBuilderString THREADS = new XContentBuilderString("threads");
        static final XContentBuilderString QUEUE = new XContentBuilderString("queue");
        static final XContentBuilderString ACTIVE = new XContentBuilderString("active");
        static final XContentBuilderString REJECTED = new XContentBuilderString("rejected");
        static final XContentBuilderString LARGEST = new XContentBuilderString("largest");
        static final XContentBuilderString COMPLETED = new XContentBuilderString("completed");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (Stats stat : stats) {
            stat.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
