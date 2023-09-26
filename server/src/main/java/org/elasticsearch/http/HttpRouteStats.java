/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.stream.IntStream;

public record HttpRouteStats(
    long requestCount,
    long totalRequestSize,
    long[] requestSizeHistogram,
    long responseCount,
    long totalResponseSize,
    long[] responseSizeHistogram,
    long[] responseTimeHistogram
) implements Writeable, ToXContentObject {

    public HttpRouteStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLongArray(), in.readVLong(), in.readVLong(), in.readVLongArray(), in.readVLongArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("requests");
        builder.field("count", requestCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalRequestSize));
        histogramToXContent(builder, "size_histogram", "bytes", requestSizeHistogram, HttpRouteStatsTracker.getBucketUpperBounds());
        builder.endObject();

        builder.startObject("responses");
        builder.field("count", responseCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalResponseSize));
        histogramToXContent(builder, "size_histogram", "bytes", responseSizeHistogram, HttpRouteStatsTracker.getBucketUpperBounds());
        histogramToXContent(
            builder,
            "handling_time_histogram",
            "millis",
            responseTimeHistogram,
            HandlingTimeTracker.getBucketUpperBounds()
        );
        builder.endObject();

        return builder.endObject();
    }

    static void histogramToXContent(XContentBuilder builder, String fieldName, String unitName, long[] histogram, int[] bucketBounds)
        throws IOException {
        assert histogram.length == bucketBounds.length + 1;
        builder.startArray(fieldName);

        int firstBucket = 0;
        long remainingCount = 0L;
        for (int i = 0; i < histogram.length; i++) {
            if (remainingCount == 0) {
                firstBucket = i;
            }
            remainingCount += histogram[i];
        }

        for (int i = firstBucket; i < histogram.length && 0 < remainingCount; i++) {
            builder.startObject();
            if (i > 0) {
                builder.humanReadableField("ge_" + unitName, "ge", ByteSizeValue.ofBytes(bucketBounds[i - 1]));
            }
            if (i < bucketBounds.length) {
                builder.humanReadableField("lt_" + unitName, "lt", ByteSizeValue.ofBytes(bucketBounds[i]));
            }
            builder.field("count", histogram[i]);
            builder.endObject();
            remainingCount -= histogram[i];
        }
        builder.endArray();
    }

    public static HttpRouteStats merge(HttpRouteStats first, HttpRouteStats second) {
        assert first.requestSizeHistogram.length == second.requestSizeHistogram.length
            && first.responseSizeHistogram.length == second.responseSizeHistogram.length
            && first.responseTimeHistogram.length == second.responseTimeHistogram.length;

        return new HttpRouteStats(
            first.requestCount + second.requestCount,
            first.totalRequestSize + second.totalRequestSize,
            IntStream.range(0, first.requestSizeHistogram.length)
                .mapToLong(i -> first.requestSizeHistogram[i] + second.requestSizeHistogram[i])
                .toArray(),
            first.responseCount + second.responseCount,
            first.totalResponseSize + second.totalResponseSize,
            IntStream.range(0, first.responseSizeHistogram.length)
                .mapToLong(i -> first.responseSizeHistogram[i] + second.responseSizeHistogram[i])
                .toArray(),
            IntStream.range(0, first.responseTimeHistogram.length)
                .mapToLong(i -> first.responseTimeHistogram[i] + second.responseTimeHistogram[i])
                .toArray()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(requestCount);
        out.writeVLong(totalRequestSize);
        out.writeVLongArray(requestSizeHistogram);
        out.writeVLong(responseCount);
        out.writeVLong(totalResponseSize);
        out.writeVLongArray(responseSizeHistogram);
        out.writeVLongArray(responseTimeHistogram);
    }
}
