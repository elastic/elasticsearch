/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * This class encapsulates the stats for a single HTTP route {@link org.elasticsearch.rest.MethodHandlers}
 *
 * @param requestCount          the number of request handled by the HTTP route
 * @param totalRequestSize      the total body size (bytes) of requests handled by the HTTP route
 * @param requestSizeHistogram  an array of frequencies of request size (bytes) in buckets with upper bounds
 *                              as returned by {@link HttpRouteStatsTracker#getBucketUpperBounds()}, plus
 *                              an extra bucket for handling size larger than the largest upper bound (currently 64MB).
 * @param responseCount         the number of responses produced by the HTTP route
 * @param totalResponseSize     the total body size (bytes) of responses produced by the HTTP route
 * @param responseSizeHistogram similar to {@code requestSizeHistogram} but for response size
 * @param responseTimeHistogram an array of frequencies of response time (millis) in buckets with upper bounds
 *                              as returned by {@link HandlingTimeTracker#getBucketUpperBounds()}, plus
 *                              an extra bucket for handling response time larger than the longest upper bound (currently 65536ms).
 */
public record HttpRouteStats(
    long requestCount,
    long totalRequestSize,
    long[] requestSizeHistogram,
    long responseCount,
    long totalResponseSize,
    long[] responseSizeHistogram,
    long[] responseTimeHistogram
) implements Writeable, ToXContentObject {

    public static final HttpRouteStats EMPTY = new HttpRouteStats(0, 0, new long[0], 0, 0, new long[0], new long[0]);

    public HttpRouteStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLongArray(), in.readVLong(), in.readVLong(), in.readVLongArray(), in.readVLongArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("requests");
        builder.field("count", requestCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalRequestSize));
        histogramToXContent(
            builder,
            "size_histogram",
            "bytes",
            ByteSizeValue::ofBytes,
            requestSizeHistogram,
            HttpRouteStatsTracker.getBucketUpperBounds()
        );
        builder.endObject();

        builder.startObject("responses");
        builder.field("count", responseCount);
        builder.humanReadableField("total_size_in_bytes", "total_size", ByteSizeValue.ofBytes(totalResponseSize));
        histogramToXContent(
            builder,
            "size_histogram",
            "bytes",
            ByteSizeValue::ofBytes,
            responseSizeHistogram,
            HttpRouteStatsTracker.getBucketUpperBounds()
        );
        histogramToXContent(
            builder,
            "handling_time_histogram",
            "millis",
            TimeValue::timeValueMillis,
            responseTimeHistogram,
            HandlingTimeTracker.getBucketUpperBounds()
        );
        builder.endObject();

        return builder.endObject();
    }

    static void histogramToXContent(
        XContentBuilder builder,
        String fieldName,
        String unitName,
        Function<Integer, Object> humanReadableValueFunc,
        long[] histogram,
        int[] bucketBounds
    ) throws IOException {
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
                builder.humanReadableField("ge_" + unitName, "ge", humanReadableValueFunc.apply(bucketBounds[i - 1]));
            }
            if (i < bucketBounds.length) {
                builder.humanReadableField("lt_" + unitName, "lt", humanReadableValueFunc.apply(bucketBounds[i]));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRouteStats that = (HttpRouteStats) o;
        return requestCount == that.requestCount
            && totalRequestSize == that.totalRequestSize
            && responseCount == that.responseCount
            && totalResponseSize == that.totalResponseSize
            && Arrays.equals(requestSizeHistogram, that.requestSizeHistogram)
            && Arrays.equals(responseSizeHistogram, that.responseSizeHistogram)
            && Arrays.equals(responseTimeHistogram, that.responseTimeHistogram);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(requestCount, totalRequestSize, responseCount, totalResponseSize);
        result = 31 * result + Arrays.hashCode(requestSizeHistogram);
        result = 31 * result + Arrays.hashCode(responseSizeHistogram);
        result = 31 * result + Arrays.hashCode(responseTimeHistogram);
        return result;
    }
}
