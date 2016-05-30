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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ThreadPoolInfo implements Streamable, Iterable<ThreadPool.Info>, ToXContent {

    private List<ThreadPool.Info> infos;

    ThreadPoolInfo() {
    }


    public ThreadPoolInfo(List<ThreadPool.Info> infos) {
        this.infos = infos;
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
    }

    public static ThreadPoolInfo readThreadPoolInfo(StreamInput in) throws IOException {
        ThreadPoolInfo info = new ThreadPoolInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        infos = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ThreadPool.Info info = new ThreadPool.Info();
            info.readFrom(in);
            infos.add(info);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(infos.size());
        for (ThreadPool.Info info : infos) {
            info.writeTo(out);
        }
    }

    static final class Fields {
        static final String THREAD_POOL = "thread_pool";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (ThreadPool.Info info : infos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
