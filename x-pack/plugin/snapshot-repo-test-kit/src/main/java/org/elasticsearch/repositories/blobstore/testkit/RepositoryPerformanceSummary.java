/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public class RepositoryPerformanceSummary implements Writeable, ToXContentFragment {

    private final long writeCount;
    private final long writeBytes;
    private final long writeThrottledNanos;
    private final long writeElapsedNanos;
    private final long readCount;
    private final long readBytes;
    private final long readWaitNanos;
    private final long maxReadWaitNanos;
    private final long readThrottledNanos;
    private final long readElapsedNanos;

    public RepositoryPerformanceSummary(
        long writeCount,
        long writeBytes,
        long writeThrottledNanos,
        long writeElapsedNanos,
        long readCount,
        long readBytes,
        long readWaitNanos,
        long maxReadWaitNanos,
        long readThrottledNanos,
        long readElapsedNanos
    ) {
        this.writeCount = writeCount;
        this.writeBytes = writeBytes;
        this.writeThrottledNanos = writeThrottledNanos;
        this.writeElapsedNanos = writeElapsedNanos;
        this.readCount = readCount;
        this.readBytes = readBytes;
        this.readWaitNanos = readWaitNanos;
        this.maxReadWaitNanos = maxReadWaitNanos;
        this.readThrottledNanos = readThrottledNanos;
        this.readElapsedNanos = readElapsedNanos;
    }

    public RepositoryPerformanceSummary(StreamInput in) throws IOException {
        writeCount = in.readVLong();
        writeBytes = in.readVLong();
        writeThrottledNanos = in.readVLong();
        writeElapsedNanos = in.readVLong();
        readCount = in.readVLong();
        readBytes = in.readVLong();
        readWaitNanos = in.readVLong();
        maxReadWaitNanos = in.readVLong();
        readThrottledNanos = in.readVLong();
        readElapsedNanos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(writeCount);
        out.writeVLong(writeBytes);
        out.writeVLong(writeThrottledNanos);
        out.writeVLong(writeElapsedNanos);
        out.writeVLong(readCount);
        out.writeVLong(readBytes);
        out.writeVLong(readWaitNanos);
        out.writeVLong(maxReadWaitNanos);
        out.writeVLong(readThrottledNanos);
        out.writeVLong(readElapsedNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("write");
        builder.field("count", writeCount);
        builder.field("total_bytes", writeBytes);
        builder.field("total_throttled_nanos", writeThrottledNanos);
        builder.field("total_elapsed_nanos", writeElapsedNanos);
        builder.endObject();

        builder.startObject("read");
        builder.field("count", readCount);
        builder.field("total_bytes", readBytes);
        builder.field("total_wait_nanos", readWaitNanos);
        builder.field("max_wait_nanos", maxReadWaitNanos);
        builder.field("total_throttled_nanos", readThrottledNanos);
        builder.field("total_elapsed_nanos", readElapsedNanos);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    static class Builder {

        private final LongAdder writeCount = new LongAdder();
        private final LongAdder writeBytes = new LongAdder();
        private final LongAdder writeThrottledNanos = new LongAdder();
        private final LongAdder writeElapsedNanos = new LongAdder();

        private final LongAdder readCount = new LongAdder();
        private final LongAdder readBytes = new LongAdder();
        private final LongAdder readWaitNanos = new LongAdder();
        private final LongAccumulator maxReadWaitNanos = new LongAccumulator(Long::max, Long.MIN_VALUE);
        private final LongAdder readThrottledNanos = new LongAdder();
        private final LongAdder readElapsedNanos = new LongAdder();

        public RepositoryPerformanceSummary build() {
            return new RepositoryPerformanceSummary(
                writeCount.longValue(),
                writeBytes.longValue(),
                writeThrottledNanos.longValue(),
                writeElapsedNanos.longValue(),
                readCount.longValue(),
                readBytes.longValue(),
                readWaitNanos.longValue(),
                Long.max(0L, maxReadWaitNanos.longValue()),
                readThrottledNanos.longValue(),
                readElapsedNanos.longValue()
            );
        }

        public void add(BlobAnalyseAction.Response response) {
            writeCount.add(1L);
            writeBytes.add(response.getWriteBytes());
            writeThrottledNanos.add(response.getWriteThrottledNanos());
            writeElapsedNanos.add(response.getWriteElapsedNanos());

            final long checksumBytes = response.getChecksumBytes();

            for (final BlobAnalyseAction.ReadDetail readDetail : response.getReadDetails()) {
                readCount.add(1L);
                readBytes.add(checksumBytes);
                readWaitNanos.add(readDetail.getFirstByteNanos());
                maxReadWaitNanos.accumulate(readDetail.getFirstByteNanos());
                readThrottledNanos.add(readDetail.getThrottledNanos());
                readElapsedNanos.add(readDetail.getElapsedNanos());
            }
        }
    }
}
