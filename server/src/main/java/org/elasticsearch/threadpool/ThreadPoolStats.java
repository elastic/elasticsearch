/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyIterator;
import static org.elasticsearch.common.collect.Iterators.single;

public record ThreadPoolStats(List<Stats> stats) implements Writeable, ChunkedToXContent, Iterable<ThreadPoolStats.Stats> {

    public record Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed)
        implements
            Writeable,
            ChunkedToXContent,
            Comparable<Stats> {

        public Stats(StreamInput in) throws IOException {
            this(in.readString(), in.readInt(), in.readInt(), in.readInt(), in.readLong(), in.readInt(), in.readLong());
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
        public int compareTo(Stats other) {
            if ((name() == null) && (other.name() == null)) {
                return 0;
            } else if ((name() != null) && (other.name() == null)) {
                return 1;
            } else if (name() == null) {
                return -1;
            } else {
                int compare = name().compareTo(other.name());
                if (compare == 0) {
                    compare = Integer.compare(threads(), other.threads());
                }
                return compare;
            }
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(name),
                threads != -1 ? single((builder, params) -> builder.field(Fields.THREADS, threads)) : emptyIterator(),
                queue != -1 ? single((builder, params) -> builder.field(Fields.QUEUE, queue)) : emptyIterator(),
                active != -1 ? single((builder, params) -> builder.field(Fields.ACTIVE, active)) : emptyIterator(),
                rejected != -1 ? single((builder, params) -> builder.field(Fields.REJECTED, rejected)) : emptyIterator(),
                largest != -1 ? single((builder, params) -> builder.field(Fields.LARGEST, largest)) : emptyIterator(),
                completed != -1 ? single((builder, params) -> builder.field(Fields.COMPLETED, completed)) : emptyIterator(),
                ChunkedToXContentHelper.endObject()
            );
        }
    }

    public ThreadPoolStats {
        Collections.sort(stats);
        stats = Collections.unmodifiableList(stats);
    }

    public ThreadPoolStats(StreamInput in) throws IOException {
        this(in.readList(Stats::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(stats);
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    static final class Fields {
        static final String THREAD_POOL = "thread_pool";
        static final String THREADS = "threads";
        static final String QUEUE = "queue";
        static final String ACTIVE = "active";
        static final String REJECTED = "rejected";
        static final String LARGEST = "largest";
        static final String COMPLETED = "completed";
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(Fields.THREAD_POOL),
            Iterators.flatMap(stats.iterator(), s -> s.toXContentChunked(params)),
            ChunkedToXContentHelper.endObject()
        );
    }
}
