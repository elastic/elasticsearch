/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record TransportActionStats(
    long requestCount,
    long totalRequestSize,
    long[] requestSizeHistogram,
    long responseCount,
    long totalResponseSize,
    long[] responseSizeHistogram
) implements Writeable, ToXContentObject {

    public TransportActionStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLongArray(), in.readVLong(), in.readVLong(), in.readVLongArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("requests");
        builder.field("count", requestCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalRequestSize));
        histogramToXContent(builder, requestSizeHistogram);
        builder.endObject();

        builder.startObject("responses");
        builder.field("count", responseCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalResponseSize));
        histogramToXContent(builder, responseSizeHistogram);
        builder.endObject();

        return builder.endObject();
    }

    static void histogramToXContent(XContentBuilder builder, long[] sizeHistogram) throws IOException {
        final int[] bucketBounds = TransportActionStatsTracker.getBucketUpperBounds();
        assert sizeHistogram.length == bucketBounds.length + 1;
        builder.startArray("histogram");

        int firstBucket = 0;
        long remainingCount = 0L;
        for (int i = 0; i < sizeHistogram.length; i++) {
            if (remainingCount == 0) {
                firstBucket = i;
            }
            remainingCount += sizeHistogram[i];
        }

        for (int i = firstBucket; i < sizeHistogram.length && 0 < remainingCount; i++) {
            builder.startObject();
            if (i > 0) {
                builder.humanReadableField("ge_bytes", "ge", ByteSizeValue.ofBytes(bucketBounds[i - 1]));
            }
            if (i < bucketBounds.length) {
                builder.humanReadableField("lt_bytes", "lt", ByteSizeValue.ofBytes(bucketBounds[i]));
            }
            builder.field("count", sizeHistogram[i]);
            builder.endObject();
            remainingCount -= sizeHistogram[i];
        }
        builder.endArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(requestCount);
        out.writeVLong(totalRequestSize);
        out.writeVLongArray(requestSizeHistogram);
        out.writeVLong(responseCount);
        out.writeVLong(totalResponseSize);
        out.writeVLongArray(responseSizeHistogram);
    }
}
