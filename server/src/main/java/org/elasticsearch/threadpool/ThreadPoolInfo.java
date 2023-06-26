/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ThreadPoolInfo implements ReportingService.Info, Iterable<ThreadPool.Info> {

    private final List<ThreadPool.Info> infos;

    public ThreadPoolInfo(List<ThreadPool.Info> infos) {
        this.infos = List.copyOf(infos);
    }

    public ThreadPoolInfo(StreamInput in) throws IOException {
        this.infos = in.readImmutableList(ThreadPool.Info::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(infos);
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
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
