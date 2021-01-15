/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

public class SpeedTestSummary implements Writeable, ToXContentFragment {

    private final long writeCount;
    private final long writeBytes;
    private final long writeThrottledNanos;
    private final long writeElapsedNanos;
    private final long readCount;
    private final long readBytes;
    private final long readWaitNanos;
    private final long readThrottledNanos;
    private final long readElapsedNanos;

    public SpeedTestSummary(
        long writeCount,
        long writeBytes,
        long writeThrottledNanos,
        long writeElapsedNanos,
        long readCount,
        long readBytes,
        long readWaitNanos,
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
        this.readThrottledNanos = readThrottledNanos;
        this.readElapsedNanos = readElapsedNanos;
    }

    public SpeedTestSummary(StreamInput in) throws IOException {
        writeCount = in.readVLong();
        writeBytes = in.readVLong();
        writeThrottledNanos = in.readVLong();
        writeElapsedNanos = in.readVLong();
        readCount = in.readVLong();
        readBytes = in.readVLong();
        readWaitNanos = in.readVLong();
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
        out.writeVLong(readThrottledNanos);
        out.writeVLong(readElapsedNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("write");
        builder.field("count", writeCount);
        builder.field("total_bytes", writeBytes);
        builder.field("throttled_nanos", writeThrottledNanos);
        builder.field("elapsed_nanos", writeElapsedNanos);
        builder.endObject();

        builder.startObject("read");
        builder.field("count", readCount);
        builder.field("total_bytes", readBytes);
        builder.field("wait_nanos", readWaitNanos);
        builder.field("throttled_nanos", readThrottledNanos);
        builder.field("elapsed_nanos", readElapsedNanos);
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
        private final LongAdder readThrottledNanos = new LongAdder();
        private final LongAdder readElapsedNanos = new LongAdder();

        public SpeedTestSummary build() {
            return new SpeedTestSummary(
                writeCount.longValue(),
                writeBytes.longValue(),
                writeThrottledNanos.longValue(),
                writeElapsedNanos.longValue(),
                readCount.longValue(),
                readBytes.longValue(),
                readWaitNanos.longValue(),
                readThrottledNanos.longValue(),
                readElapsedNanos.longValue()
            );
        }

        public void add(BlobSpeedTestAction.Response response) {
            writeCount.add(1L);
            writeBytes.add(response.getWriteBytes());
            writeThrottledNanos.add(response.getWriteThrottledNanos());
            writeElapsedNanos.add(response.getWriteElapsedNanos());

            final long checksumBytes = response.getChecksumBytes();

            for (final BlobSpeedTestAction.ReadDetail readDetail : response.getReadDetails()) {
                readCount.add(1L);
                readBytes.add(checksumBytes);
                readWaitNanos.add(readDetail.getFirstByteNanos());
                readThrottledNanos.add(readDetail.getThrottledNanos());
                readElapsedNanos.add(readDetail.getElapsedNanos());
            }
        }
    }
}
